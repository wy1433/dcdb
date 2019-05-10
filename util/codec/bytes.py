import unittest
import struct
from util import error

# EncodeBytes guarantees the encoded value is in ascending order for comparison,
def EncodeBytes(b = "", v_str = ""):
    '''
    @param v_str: str data to encode
    @attention: as b'\0' used for codec's separator, so data could not contains '\0
    '''
    if b is None:
        b = ""
    return b  + b'\0' + v_str


# DecodeBytes decodes bytes which is encoded by EncodeBytes before,
# returns the leftover bytes and decoded value if no error.
# `buf` is used to buffer data to avoid the cost of makeslice in decodeBytes when DecodeBytes is called by Decoder.DecodeOne.
def DecodeBytes(data):
    '''
    @type data: str
    @rtype: str, str, error
    @return: left_str, right_str, error
    '''
    info = data.rsplit(b'\0', 1)
    if len(info) != 2:
        return None, None, error.BaseError("DecodeBytes err. \0 not found.")
    else:
        return info[0], info[1], None