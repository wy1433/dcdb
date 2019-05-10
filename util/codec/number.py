import unittest
import struct
from util import error

signMask = 0x8000000000000000
uint64Mask = 0xFFFFFFFFFFFFFFFF


# EncodeIntToCmpUint make int v to comparable uint type
def EncodeIntToCmpUint(v_int64):
    return (v_int64 ^ signMask) & uint64Mask


# DecodeCmpUintToInt decodes the u that encoded by EncodeIntToCmpUint
def DecodeCmpUintToInt(v_uint64):
    if v_uint64 >= signMask:
        return v_uint64 ^ signMask 
    else:
        return v_uint64 ^ -signMask 


# EncodeInt appends the encoded value to slice b and returns the appended slice.
# EncodeInt guarantees that the encoded value is in ascending order for comparison.
def EncodeInt(b, v_int64):
    '''
    @param b: byte strings
    @param v_int64: int type which is to be append to b
    @return: byte strings
    '''
    v_uint64 = EncodeIntToCmpUint(v_int64)
    data = '%016x' % v_uint64
    return b + data


# EncodeIntDesc appends the encoded value to slice b and returns the appended slice.
# EncodeIntDesc guarantees that the encoded value is in descending order for comparison.
def EncodeIntDesc(b , v_int64):
    '''
    @param b: byte strings
    @param v_int64: int type which is to be append to b
    @return: byte strings
    '''
    v_uint64 = EncodeIntToCmpUint(v_int64) ^ uint64Mask
    data = '%016x' % v_uint64
    print '-----------', v_int64, data
    return b + data


# DecodeInt decodes value encoded by EncodeInt before.
# It returns the leftover un-decoded slice, decoded value if no error.
def DecodeInt(b):
    if len(b) < 16 :
        return None, None, error.BaseError("insufficient bytes to decode value")
    u = int(b[-16:], 16)
    v = DecodeCmpUintToInt(u)
    b = b[:-16]
    return b, v, None


# DecodeIntDesc decodes value encoded by EncodeInt before.
# It returns the leftover un-decoded slice, decoded value if no error.
def DecodeIntDesc(b):
    if len(b) < 16 :
        return None, None, error.BaseError("insufficient bytes to decode value")
    u = int(b[-16:], 16)  ^ uint64Mask
    v = DecodeCmpUintToInt(u)
    b = b[:-16]
    return b, v, None


# EncodeUint appends the encoded value to slice b and returns the appended slice.
# EncodeUint guarantees that the encoded value is in ascending order for comparison.
def EncodeUint(b, v_uint64):
    data = '%016x' % v_uint64
    return b + data


# EncodeUintDesc appends the encoded value to slice b and returns the appended slice.
# EncodeUintDesc guarantees that the encoded value is in descending order for comparison.
def EncodeUintDesc(b, v_uint64):
    data = '%016x' % (v_uint64 ^ uint64Mask)
    return b + data


# DecodeUint decodes value encoded by EncodeUint before.
# It returns the leftover un-decoded slice, decoded value if no error.
def DecodeUint(b):
    if len(b) < 16 :
        return None, None, error.BaseError("insufficient bytes to decode value")
    u = int(b[-16:], 16)
    b = b[:-16]
    return b, u, None

    
# DecodeUintDesc decodes value encoded by EncodeInt before.
# It returns the leftover un-decoded slice, decoded value if no error.
def DecodeUintDesc(b):
    if len(b) < 16 :
        return None, None, error.BaseError("insufficient bytes to decode value")
    u = int(b[-16:], 16) ^ uint64Mask
    b = b[:-16]
    return b, u, None


# EncodeInt appends the encoded value to slice b and returns the appended slice.
# EncodeInt guarantees that the encoded value is in ascending order for comparison.
def EncodeInt2(b, v_int64):
    '''
    @param b: byte strings
    @param v_int64: int type which is to be append to b
    @return: byte strings
    '''
    v_uint64 = EncodeIntToCmpUint(v_int64)
    data = struct.pack(">Q", v_uint64)
    return b + data


# EncodeIntDesc appends the encoded value to slice b and returns the appended slice.
# EncodeIntDesc guarantees that the encoded value is in descending order for comparison.
def EncodeIntDesc2(b , v_int64):
    '''
    @param b: byte strings
    @param v_int64: int type which is to be append to b
    @return: byte strings
    '''
    v_uint64 = EncodeIntToCmpUint(v_int64)
    data = struct.pack(">Q", v_uint64 ^ uint64Mask)
    return b + data


# DecodeInt decodes value encoded by EncodeInt before.
# It returns the leftover un-decoded slice, decoded value if no error.
def DecodeInt2(b):
    if len(b) < 8 :
        return None, None, error.BaseError("insufficient bytes to decode value")
    u = struct.unpack(">Q", b[-8:])[0]
    v = DecodeCmpUintToInt(u)
    b = b[:-8]
    return b, v, None


# DecodeIntDesc decodes value encoded by EncodeInt before.
# It returns the leftover un-decoded slice, decoded value if no error.
def DecodeIntDesc2(b):
    if len(b) < 8 :
        return None, None, error.BaseError("insufficient bytes to decode value")
    u = struct.unpack(">Q", b[-8:])[0] ^ uint64Mask
    v = DecodeCmpUintToInt(u)
    b = b[:-8]
    return b, v, None


# EncodeUint appends the encoded value to slice b and returns the appended slice.
# EncodeUint guarantees that the encoded value is in ascending order for comparison.
def EncodeUint2(b, v_uint64):
    data = struct.pack(">Q", v_uint64)
    return b + data


# EncodeUintDesc appends the encoded value to slice b and returns the appended slice.
# EncodeUintDesc guarantees that the encoded value is in descending order for comparison.
def EncodeUintDesc2(b, v_uint64):
    data = struct.pack(">Q", v_uint64 ^ uint64Mask)
    return b + data


# DecodeUint decodes value encoded by EncodeUint before.
# It returns the leftover un-decoded slice, decoded value if no error.
def DecodeUint2(b):
    if len(b) < 8 :
        return None, None, error.BaseError("insufficient bytes to decode value")
    u = struct.unpack(">Q", b[-8:])[0]
    b = b[:-8]
    return b, u, None

    
# DecodeUintDesc decodes value encoded by EncodeInt before.
# It returns the leftover un-decoded slice, decoded value if no error.
def DecodeUintDesc2(b):
    if len(b) < 8 :
        return None, None, error.BaseError("insufficient bytes to decode value")
    u = struct.unpack(">Q", b[-8:])[0] ^ uint64Mask
    b = b[:-8]
    return b, u, None


# ## for unittest
min_int64 = -9223372036854775808  # -2**63
max_int64 = 9223372036854775807  # 2**63-1


class TestClass(unittest.TestCase):

    def codec(self, v):
        u = EncodeIntToCmpUint(v)  # +2**63
        print "v=%x, u=%x" % (v, u)
        self.assertEqual(v, DecodeCmpUintToInt(u))
    
    def comp(self, v1, v2):
        u1 = EncodeIntToCmpUint(v1)
        u2 = EncodeIntToCmpUint(v2)
        print "v1=%d, v2=%d, u1=%d, u2=%d" % (v1, v2, u1, u2)
     
        if (v1 < v2):
            self.assertLess(u1, u2)
        elif v1 == v2 :
            self.assertEqual(u1, u2)
        else:
            self.assertGreater(u1, u2)
        
    def test_codec(self):
        self.codec(min_int64)
        self.codec(max_int64)
        self.codec(0L)
        self.codec(256L)
        self.codec(-256L)
        self.codec(999L)
        self.codec(-999L)
       
    def test_comp(self):
        self.comp(min_int64, min_int64)
        self.comp(0, 0)
        self.comp(max_int64, max_int64)
        self.comp(min_int64, 0)
        self.comp(0, max_int64)
        self.comp(min_int64, max_int64)
        self.comp(-255, 255)
        
    def test_int(self):
        self.assertEqual(DecodeInt(EncodeInt("key", -10)), ("key", -10, None))
        self.assertEqual(DecodeInt(EncodeInt("key", 10)), ("key", 10, None))
        self.assertLess(EncodeInt("key", -10), EncodeInt("key", 10))
        self.assertLess(EncodeInt("key", 10), EncodeInt("key", 20))
        
    def test_int_desc(self):
        self.assertEqual(DecodeIntDesc(EncodeIntDesc("key", -10)), ("key", -10, None))
        self.assertEqual(DecodeIntDesc(EncodeIntDesc("key", 10)), ("key", 10, None))
        self.assertGreater(EncodeIntDesc("key", -10), EncodeIntDesc("key", 10))
        self.assertGreater(EncodeIntDesc("key", 10), EncodeIntDesc("key", 20))
        
        
if __name__ == '__main__':
    unittest.main()
