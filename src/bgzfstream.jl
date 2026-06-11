# BGZFStream
# ==========

# Internal details
# ----------------
#
# When reading data from an input, compressed data will be read to a buffer
# (compressed block) and then inflated into a decompressed block at a time.
# When writing data to an output, raw data will be deflated into a compressed
# block and then written to the output immediately.  Each data block is no
# larger than 64 KiB before and after compression.
#
# Read mode (stream.mode = READ_MODE)
# -----------------------------------
#
#          compressed block          decompressed block
# stream   +---------------+         +---------------+
# .io ---> |xxxxxxx        | ------> |xxxxxxxxxxx    | --->
#     read +---------------+ inflate +---------------+ read
#                                    |------>| block.position ∈ [0, 64K)
#                                    |--------->| block.size ∈ [0, 64K]
#
# Write mode (stream.mode = WRITE_MODE)
# -------------------------------------
#
#          compressed block          decompressed block
# stream   +---------------+         +---------------+
# .io <--- |xxxxxxx        | <------ |xxxxxxxx       | <---
#    write +---------------+ deflate +---------------+ write
#                                    |------>| block.position ∈ [0, 64K)
#                                    |------------->| block.size = 64K - 256
# - xxx: used data
# - 64K: 65536 (= BGZF_MAX_BLOCK_SIZE = 64 * 1024)

mutable struct Block
    # space for the compressed block
    compressed_block::Vector{UInt8}

    # space for the decompressed block
    decompressed_block::Vector{UInt8}

    # block offset in a file (this is always 0 for a pipe stream)
    block_offset::Int

    # the next reading byte position in a block
    position::Int

    # number of available bytes in the decompressed block
    size::Int

    # number of valid bytes in compressed_block (read mode only)
    compressed_size::Int

    # LibDeflate compressor (write) or decompressor (read)
    libdeflate::Union{LibDeflate.Compressor, LibDeflate.Decompressor}
end

function Block(mode)
    compressed_block = Vector{UInt8}(undef, BGZF_MAX_BLOCK_SIZE)
    decompressed_block = Vector{UInt8}(undef, BGZF_MAX_BLOCK_SIZE)

    if mode == READ_MODE
        libdeflate = LibDeflate.Decompressor()
        size = 0
    else
        libdeflate = LibDeflate.Compressor()
        size = BGZF_SAFE_BLOCK_SIZE
    end

    return Block(compressed_block, decompressed_block, 0, 1, size, 0, libdeflate)
end

# Stream type for the BGZF compression format.
mutable struct BGZFStream{T<:IO} <: IO
    # underlying IO stream
    io::T

    # read/write mode
    mode::UInt8

    # compressed & decompressed blocks with metadata and zstream
    blocks::Vector{Block}

    # current block index
    block_index::Int

    # index of the last block loaded
    last_block_index::Int

    # whether stream is open
    isopen::Bool

    # callback function called when closing the stream
    onclose::Function
end

function BGZFStream(io, mode, blocks, block_index, isopen, onclose) # This method maintains compatibility after the inclusion of the `last_block_index` field to `BGZFStream`.
    return BGZFStream(io, mode, blocks, block_index, 0, isopen, onclose)
end

# BGZF blocks are no larger than 64 KiB before and after compression.
const BGZF_MAX_BLOCK_SIZE = UInt(64 * 1024)

# BGZF_MAX_BLOCK_SIZE minus "margin for safety"
# NOTE: Data block will become slightly larger after deflation when bytes are
# randomly distributed.
const BGZF_SAFE_BLOCK_SIZE = UInt(BGZF_MAX_BLOCK_SIZE - 256)

# Read mode:  inflate and read a BGZF file
# Write mode: deflate and write a BGZF file
const READ_MODE  = 0x00
const WRITE_MODE = 0x01

"""
    BGZFStream(io::IO[, mode::AbstractString="r"])
    BGZFStream(filename::AbstractString[, mode::AbstractString="r"])

Create an I/O stream for the BGZF compression format.

The first argument is either an `IO` object or a filename. If `mode` is `"r"`
(read) the BGZF stream will be in read mode and decompress the underlying BGZF
blocks while reading. In read mode, `BGZFStream` supports the `seek` operation
using a virtual file offset (see `VirtualOffset`). If `mode` is `"w"` (write)
or `"a"` (append) the BGZF stream will be in write mode and compress written
data to BGZF blocks.
"""
function BGZFStream(io::IO, mode::AbstractString="r")
    if mode ∉ ("r", "w", "a")
        throw(ArgumentError("invalid mode: \"", mode, "\""))
    end

    # the number of parallel workers
    mode′ = mode == "r" ? READ_MODE : WRITE_MODE
    if mode′ == READ_MODE
        blocks = [Block(mode′) for _ in 1:Threads.nthreads()]
    else
        # Write mode is not (yet?) multi-threaded.
        blocks = [Block(mode′)]
    end
    return BGZFStream(io, mode′, blocks, 1, true, io -> close(io))
end

function BGZFStream(filename::AbstractString, mode::AbstractString = "r")
    if mode ∉ ("r", "w", "a")
        throw(ArgumentError("invalid mode: '", mode, "'"))
    end
    return BGZFStream(open(filename, mode), mode)
end

function Base.open(::Type{BGZFStream}, filepath::AbstractString, mode::AbstractString = "r")
    return BGZFStream(filepath, mode)
end

"""
    virtualoffset(stream::BGZFStream)

Return the current virtual file offset of `stream`.
"""
function virtualoffset(stream::BGZFStream)
    if stream.mode == READ_MODE
        i = ensure_buffered_data(stream)
        if i == 0
            block = stream.blocks[stream.block_index]
        else
            block = stream.blocks[i]
        end
    else
        block = stream.blocks[1]
    end
    return VirtualOffset(block.block_offset, block.position - 1)
end

function virtualoffset(stream::BGZFStream{T}) where {T<:Base.AbstractPipe}
    throw(ArgumentError("virtualoffset is not supported for a pipe stream"))
end

function Base.show(io::IO, stream::BGZFStream)
    print(io,
        summary(stream),
        "(<",
        "mode=", stream.mode == READ_MODE ? "read" : "write",
        ">)")
end

function Base.isopen(stream::BGZFStream)
    return stream.isopen
end

function Base.close(stream::BGZFStream)
    if stream.mode == WRITE_MODE
        if stream.blocks[1].position > 1
            write_blocks!(stream)
        end
        write(stream.io, EOF_BLOCK)
    end
    stream.isopen = false
    stream.onclose(stream.io)
    return
end

function Base.flush(stream::BGZFStream)
    if stream.mode == WRITE_MODE
        flush(stream.io)
    end
    return
end

function Base.eof(stream::BGZFStream)
    if stream.mode == READ_MODE
        return ensure_buffered_data(stream) == 0
    else
        return true
    end
end

function Base.seekstart(stream::BGZFStream)
    seek(stream, VirtualOffset(0, 0))
end

function Base.seek(stream::BGZFStream, voffset::VirtualOffset)
    if stream.mode == WRITE_MODE
        throw(ArgumentError("BGZFStream in write mode is not seekable"))
    end
    block_offset, inblock_offset = offsets(voffset)
    seek(stream.io, block_offset)
    read_blocks!(stream)
    block = first(stream.blocks)
    if inblock_offset ≥ block.size
        throw(ArgumentError("too large in-block offset"))
    end
    block.block_offset = block_offset
    block.position = inblock_offset + 1
    return
end

function Base.seek(stream::BGZFStream{T}, voffset::VirtualOffset) where {T<:Base.AbstractPipe}
    throw(ArgumentError("seek is not supported for a pipe stream"))
end

function Base.read(stream::BGZFStream, ::Type{UInt8})
    if !isopen(stream)
        throw(ArgumentError("stream is already closed"))
    elseif stream.mode != READ_MODE
        throw(ArgumentError("stream is not readable"))
    end
    block_index = ensure_buffered_data(stream)
    if block_index == 0
        throw(EOFError())
    end
    block = stream.blocks[block_index]
    byte = block.decompressed_block[block.position]
    block.position += 1
    return byte
end

function Base.write(stream::BGZFStream, byte::UInt8)
    if !isopen(stream)
        throw(ArgumentError("stream is already closed"))
    elseif stream.mode != WRITE_MODE
        throw(ArgumentError("stream is not writable"))
    end
    block = stream.blocks[1]
    block.decompressed_block[block.position] = byte
    block.position += 1
    if block.position > block.size
        write_blocks!(stream)
    end
    return 1
end

function Base.unsafe_read(stream::BGZFStream, p::Ptr{UInt8}, n::UInt)
    if !isopen(stream)
        throw(ArgumentError("stream is already closed"))
    elseif stream.mode != READ_MODE
        throw(ArgumentError("stream is not readable"))
    end
    p_end = p + n
    while p < p_end
        i = ensure_buffered_data(stream)
        if i == 0
            throw(EOFError())
        end
        @inbounds block = stream.blocks[i]
        len = min(p_end - p, block.size - block.position + 1)
        src = pointer(block.decompressed_block, block.position)
        memcpy(p, src, len)
        block.position += len
        p += len
    end
end

function Base.unsafe_write(stream::BGZFStream, p::Ptr{UInt8}, n::UInt)
    if !isopen(stream)
        throw(ArgumentError("stream is already closed"))
    elseif stream.mode != WRITE_MODE
        throw(ArgumentError("stream is not writable"))
    end
    block = stream.blocks[1]
    p_end = p + n
    while p < p_end
        len = min(p_end - p, block.size - block.position + 1)
        dst = pointer(block.decompressed_block, block.position)
        memcpy(dst, p, len)
        block.position += len
        if block.position > block.size
            write_blocks!(stream)
        end
        p += len
    end
    return Int(n)
end


# Internal functions
# ------------------

# Ensure buffered data (at least 1 byte) for reading and return the block index
# if available or 0 otherwise.
@inline function ensure_buffered_data(stream)
    #@assert stream.mode == READ_MODE
    @label doit
    while stream.block_index < stream.last_block_index
        @inbounds block = stream.blocks[stream.block_index]
        if block.position ≤ block.size
            return stream.block_index
        end
        stream.block_index += 1
    end
    if stream.block_index == stream.last_block_index
        @inbounds block = stream.blocks[stream.block_index]
        if block.position ≤ block.size
            return stream.block_index
        end
    end
    if !eof(stream.io)
        read_blocks!(stream)
        @goto doit
    end
    return 0
end

# A wrapper of memcpy.
function memcpy(dst, src, len)
    ccall(
        :memcpy,
        Ptr{Cvoid},
        (Ptr{Cvoid}, Ptr{Cvoid}, Csize_t),
        dst, src, len)
end

struct BGZFDataError <: Exception
    message::AbstractString
end

# Throw a BGZFDataError exception with the given error message.
function bgzferror(message::AbstractString="malformed BGZF data")
    throw(BGZFDataError(message))
end

# Read and decompress blocks.
function read_blocks!(stream)
    @assert stream.mode == READ_MODE

    # read BGZF blocks in sequence
    n_blocks = 0
    has_position = true
    try
        position(stream.io)
    catch
        has_position = false
    end
    while n_blocks < length(stream.blocks) && !eof(stream.io)
        block = stream.blocks[n_blocks += 1]
        stream.last_block_index = n_blocks
        if has_position
            block.block_offset = position(stream.io)
        end
        block.position = 1
        block.compressed_size = read_bgzf_block!(stream.io, block.compressed_block)
    end

    # decompress blocks in parallel
    had_error = fill(false, n_blocks)
    error_msgs = fill("", n_blocks)
    Threads.@threads for i in 1:n_blocks
        block = stream.blocks[i]
        decompressor = block.libdeflate::LibDeflate.Decompressor
        in_data = block.compressed_block
        GC.@preserve in_data begin
            result = LibDeflate.unsafe_gzip_decompress!(
                decompressor,
                block.decompressed_block,
                UInt(BGZF_MAX_BLOCK_SIZE),
                pointer(in_data),
                block.compressed_size,
                nothing,
            )
        end
        if result isa LibDeflate.LibDeflateError
            had_error[i] = true
            error_msgs[i] = string(result)
        else
            block.size = result.len
        end
    end

    for i in 1:n_blocks
        if had_error[i]
            error("LibDeflate failed to decompress BGZF block $i: $(error_msgs[i])")
        end
        @assert stream.blocks[i].size ≤ BGZF_MAX_BLOCK_SIZE
    end

    stream.block_index = 1
    return
end

# Read a BGZF block from `input`.
function read_bgzf_block!(input, block)
    # TODO: check the number of read bytes

    # +---+---+---+---+---+---+---+---+---+---+
    # |ID1|ID2|CM |FLG|     MTIME     |XFL|OS | (more-->)
    # +---+---+---+---+---+---+---+---+---+---+
    unsafe_read(input, pointer(block), 10)
    id1_ok = block[1] == 0x1f
    id2_ok = block[2] == 0x8b
    cm_ok  = block[3] == 0x08
    flg_ok = block[4] == 0x04
    if !id1_ok || !id2_ok
        bgzferror("invalid gzip identifier")
    elseif !cm_ok
        bgzferror("invalid compression method")
    elseif !flg_ok
        bgzferror("invalid flag")
    end

    # +---+---+=================================+
    # | XLEN  |...XLEN bytes of "extra field"...| (more-->)
    # +---+---+=================================+
    unsafe_read(input, pointer(block, 11) , 2)
    xlen = UInt16(block[11]) | UInt16(block[12]) << 8
    unsafe_read(input, pointer(block, 13), xlen)
    bsize::Int = 0
    pos = 12
    while pos < 12 + xlen
        si1 = block[pos+1]
        si2 = block[pos+2]
        slen = UInt16(block[pos+3]) | UInt16(block[pos+4]) << 8
        if si1 == 0x42 || si2 == 0x43
            if slen != 2
                bgzferror("invalid subfield length")
            end
            bsize = (UInt16(block[pos+5]) | UInt16(block[pos+6]) << 8) + 1
        end
        # skip this field
        pos += 4 + slen
    end
    if bsize == 0
        bgzferror("no block size")
    end

    # +=======================+---+---+---+---+---+---+---+---+
    # |...compressed blocks...|     CRC32     |     ISIZE     |
    # +=======================+---+---+---+---+---+---+---+---+
    size = bsize - 1 - xlen - 19 + 8
    unsafe_read(input, pointer(block, 13 + xlen), size)

    if eof(input) && !is_eof_block(block)
        bgzferror("no end-of-file marker (maybe a truncated file)")
    end

    return bsize
end

function write_blocks!(stream)
    @assert stream.mode == WRITE_MODE

    n_blocks = length(stream.blocks)
    @assert n_blocks == 1

    for i in 1:n_blocks
        block = stream.blocks[i]
        compressor = block.libdeflate::LibDeflate.Compressor
        n_uncompressed = block.position - 1
        compressed_block = block.compressed_block
        decompressed_block = block.decompressed_block

        # Compress raw DEFLATE into compressed_block[19..] (after 18-byte BGZF header)
        n_compressed = GC.@preserve compressed_block decompressed_block begin
            LibDeflate.unsafe_compress!(
                compressor,
                pointer(compressed_block, 19),
                UInt(BGZF_MAX_BLOCK_SIZE - 18 - 8),
                pointer(decompressed_block),
                n_uncompressed,
            )
        end
        if n_compressed isa LibDeflate.LibDeflateError
            error("LibDeflate failed to compress BGZF block: $(n_compressed)")
        end

        # Append CRC32 and ISIZE after compressed data
        crc = GC.@preserve decompressed_block LibDeflate.unsafe_crc32(
            pointer(decompressed_block), n_uncompressed
        )
        GC.@preserve compressed_block begin
            unsafe_store!(Ptr{UInt32}(pointer(compressed_block, 19 + n_compressed)),     htol(crc))
            unsafe_store!(Ptr{UInt32}(pointer(compressed_block, 19 + n_compressed + 4)), htol(UInt32(n_uncompressed)))
        end

        blocksize = 18 + n_compressed + 8
        fix_header!(compressed_block, blocksize)
        nb = unsafe_write(stream.io, pointer(compressed_block), blocksize)
        if nb != blocksize
            error("failed to write a BGZF block")
        end
        if !isa(stream.io, Pipe)
            block.block_offset = position(stream.io)
        end
        block.position = 1
    end
end

function fix_header!(block, blocksize)
    copyto!(block,
            # ID1   ID2    CM   FLG  |<--     MTIME    -->|   XFL    OS
            [0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff])
    copyto!(block, 11,
            #  XLEN    S1    S2    SLEN          BSIZE
            reinterpret(UInt8, [0x0006, 0x4342, 0x0002, UInt16(blocksize - 1)]))
end

# end-of-file marker block (used for detecting unintended file truncation)
const EOF_BLOCK = [
    0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00,
    0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00,
    0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00
]

# Return true iff the block is a end-of-file marker.
function is_eof_block(block)
    if length(block) < length(EOF_BLOCK)
        return false
    end
    for i in 1:lastindex(EOF_BLOCK)
        if block[i] != EOF_BLOCK[i]
            return false
        end
    end
    return true
end

