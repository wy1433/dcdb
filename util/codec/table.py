import unittest
from meta.model import FieldType, IndexType
from number import EncodeInt, DecodeInt
from mylog import logger

def EncodeRowid(rowid):
    '''
    @param rowid: int
    @return: str
    '''
    # return '%08x' % rowid
    return EncodeInt("", rowid)


def DecodeRowid(s):
    '''
    @param s: str
    @return: int
    '''
    return int(s)


def EncodeValue(value, fieldType):
    '''
    @param value: str
    @param fieldType: FieldType
    @return: str
    '''
    v = value
    if fieldType == FieldType.STR:
        v =  value
    elif fieldType == FieldType.INT:
        v_int64 = int(value)
        v = EncodeInt("", v_int64)
#     logger.debug('value=%s, encode_v=%s', value, v)
    return v

    
def DecodeValue(s, fieldType):
    '''
    @param s: str
    @param fieldType: FieldType
    @return: str
    '''
    if fieldType == FieldType.STR:
        return s
    elif fieldType == FieldType.INT:
        b, v, err = DecodeInt(s)
        assert err is None
        return v


def EncodeIndexKey(rowid, value, fieldType, indexType):
    '''
    @param rowid: int
    @param value: value of column
    @param fieldType: FieldType
    @param indexType: IndexType
    @return: str
    '''
    v = EncodeValue(value, fieldType)
    if indexType == IndexType.UNIQUE:
        s = v
    else:
        r = EncodeRowid(rowid)
        s = '%s.%s' % (v, r)
    return s


def DecodeIndexKey(index_key, fieldType, indexType):
    if indexType == IndexType.UNIQUE:
        s = index_key
        r = None
    else:
        s, r = index_key.rsplit(".", 1)
    
    v = DecodeValue(s, fieldType)
    return v, r

# ## for unittest
# class TestClass(unittest.TestCase):
#     def MustEncodeKeyOk(self, h, d):
#         _, err = EncodeKey(h, d)
#         self.assertEqual(err, None)
#         
#     def MustEncodeKeyErr(self, h, d):
#         _, err = EncodeKey(h, d)
#         self.assertNotEqual(err, None)
#         
#     def MustDecodeKeyOk(self, k, expect):
#         _, data, err = DecodeKey(k)
#         self.assertEqual(data, expect)
#         self.assertEqual(err, None)
#         
#     def MustDecodeKeyErr(self,k, expect):
#         _, data, err = DecodeKey(k)
#         self.assertNotEqual(err, None)
#         
#     def test_EncodeKey(self):
#         row_head = KeyHead(10, 20, 0)
#         self.MustEncodeKeyOk(row_head, 400)
#         self.MustEncodeKeyErr(row_head, "value")
#         
#         index_head = KeyHead(10, 0, 20)
#         self.MustEncodeKeyErr(index_head, 400)
#         self.MustEncodeKeyOk(index_head, "value")
#     
#     def test_DecodeKey(self):
#         row_head = KeyHead(10, 20, 0)
#         k, err = EncodeKey(row_head, 400)
#         self.MustDecodeKeyOk(k, 400)
#         
#         index_head = KeyHead(10, 0, 20)
#         k, err = EncodeKey(index_head, "value")
#         self.MustDecodeKeyOk(k, "value")

        
if __name__ == '__main__':
    pass
#     unittest.main()
