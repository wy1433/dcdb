# from enum import Enum, unique

import sys
# sys.path.append("..")
import struct
from collections import namedtuple

from mylog import logger
from util.error import ErrLocked, BaseError
from util import codec
import interface.gen_py.kvrpcpb_pb2 as kvrpcpb
   
typePut = 0
typeDelete = 1
typeRollback = 2

lockVer = 0xFFFFFFFFFFFFFFFF  # MaxUint64
nil = None


# ErrInvalidEncodedKey describes parsing an invalid format of EncodedKey.
class ErrInvalidEncodedKey(BaseError):

    def ERROR(self):
        return "invalid encoded key"

# mvccEncode returns the encoded key.
def mvccEncode(key, ver):
    if key is None or key =="":
        return None
    b = codec.EncodeBytes(key, "")
    ret = codec.EncodeUintDesc(b, ver)
    return ret


# mvccDecode parses the origin key and version of an encoded key, if the encoded key is a meta key,
# just returns the origin key.
def mvccDecode(encodedKey):
    b, ver, err = codec.DecodeUintDesc(encodedKey)
    if err != None :
        # should never happen
        return None, 0, err
    key, _, err = codec.DecodeBytes(b)
    return key, ver, None

class mvccValue():
    def __init__(self, valueType = 0, startTS = 0, commitTS = 0, value = ""):
        '''
        @type valueType mvccValueType
        @type startTS   uint64
        @type commitTS  uint64
        @type value     []byte
        '''
        self.valueType = valueType
        self.startTS = startTS
        self.commitTS = commitTS
        self.value = value
          
    # MarshalBinary implements encoding.BinaryMarshaler interface.
    def MarshalBinary(self):
        '''
            @rtype: []byte
        '''
        mh = marshalHelper()
        mh.WriteNumber(self.valueType)
        mh.WriteNumber(self.startTS)
        mh.WriteNumber(self.commitTS)
        mh.WriteSlice(self.value)
        return str(mh.buf), mh.err
      
    # UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
    def UnmarshalBinary(self, data):
        mh = marshalHelper(buf=bytearray(data))
        self.valueType = mh.ReadNumber()
        self.startTS = mh.ReadNumber()
        self.commitTS = mh.ReadNumber()
        self.value = mh.ReadSlice()
        return mh.err
    
    def __repr__(self):
        return "mvccValue[valueType=%d,startTS=%d,commitTS=%d,value=%s]" % (
            self.valueType ,
            self.startTS,
            self.commitTS,
            self.value
        )


class mvccLock():
    def __init__(self, startTS = None, primary = None, value = None, op = None, ttl = None):
        '''
        @type startTS uint64
        @type primary []byte
        @type value   []byte
        @type op      kvrpcpb.Op
        @type ttl     uint64
        
        '''
        self.startTS = startTS
        self.primary = primary
        self.value = value
        self.op = op
        self.ttl = ttl
        
        self.buf = bytearray()
       

    # MarshalBinary implements encoding.BinaryMarshaler interface.
    def MarshalBinary(self):
        '''
        @rtype: []byte
        '''
        mh = marshalHelper()
        mh.WriteNumber(self.startTS)
        mh.WriteSlice(self.primary)
        mh.WriteSlice(self.value)
        mh.WriteNumber(self.op)
        mh.WriteNumber(self.ttl)
        return str(mh.buf), mh.err
     
    # UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
    def UnmarshalBinary(self, data):
        mh = marshalHelper(buf=bytearray(data))
        self.startTS = mh.ReadNumber()
        self.primary = mh.ReadSlice()
        self.value = mh.ReadSlice()
        self.op = mh.ReadNumber()
        self.ttl = mh.ReadNumber()
        return mh.err

    # lockErr returns ErrLocked.
    def lockErr(self, key):
        return ErrLocked(key, self.primary, self.startTS, self.ttl)
    
    def check(self, ts, key):
        # ignore when ts is older than lock or lock's type is Lock.
        if self.startTS > ts or self.op == kvrpcpb.Lock:
            return ts, None
        
        # for point get latest version.
#         if ts == lockVer and self.primary == key:
#             return self.startTS - 1, None
        
        return 0, self.lockErr(key)
        

class marshalHelper():
    def __init__(self, buf = None, offset = 0):
        '''
        @type buf: bytearray
        '''
        if buf:
            self.buf = buf
        else:
            self.buf = bytearray(0)
            
        self.offset = offset
        self.err = None
            

    def WriteNumber(self, n):
        '''
        @type n:  uint64
        @attention: number is only support uint64(unsigned long long) now
        '''
        if n is None:
            n = 0
        tmp_buf = struct.pack(">Q", n)
        self.buf.extend(tmp_buf)
        
    def WriteSlice(self, s):
        '''
        @type s: byte[]
        '''
        if s  is None:
            s = ""        
        sz = len(s)
        tmp_buf = struct.pack(">Q%ds" % sz, sz, s)
        self.buf.extend(tmp_buf)

    
    def ReadNumber(self):
        n = struct.unpack_from(">Q", self.buf, self.offset)[0]
        self.offset += struct.calcsize(">Q")
        return n
        
    
    def ReadSlice(self):
        sz = self.ReadNumber()
        s = struct.unpack_from(">%ds" % sz, self.buf, self.offset)[0]
        self.offset += sz
        return s


# Pair is a KV pair read from MvccStore or an error if any occurs.
Pair = namedtuple('Pair', ['Key', 'Value','Err'])

if __name__ == '__main__':
    data = mvccEncode('x', 5)
    print repr(bytearray(data))
    print mvccDecode(data)
    
    