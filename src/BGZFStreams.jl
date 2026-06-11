module BGZFStreams

export
    BGZFStream,
    VirtualOffset,
    BGZFDataError,
    virtualoffset

import LibDeflate

include("virtualoffset.jl")
include("bgzfstream.jl")

end # module
