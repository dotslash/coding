import protos.test_pb2 as testpb

"""
message Simple {
  string name = 1;
  int64 id = 2;
  repeated string tag = 3;
}
"""


class ProtoDeserialize:
    index: int
    byte_array: bytes

    types = [
        'varint',
        'na',
        'string'
    ]
    bytes_lsb3 = 0b111

    @staticmethod
    def bytes_msb5(b):
        return 0b11111 & (b >> 3)

    def __init__(self, byte_array: bytes):
        self.byte_array = byte_array
        self.index = 0

    def _parse_type_and_field_num(self):
        b = self.byte_array[self.index]
        _type = ProtoDeserialize.types[b & ProtoDeserialize.bytes_lsb3]
        _field_num = ProtoDeserialize.bytes_msb5(b)
        self.index += 1
        return _type, _field_num

    def _parse_string(self):
        length = self.byte_array[self.index]
        print(length)
        self.index += 1
        ret: bytes = self.byte_array[self.index: self.index + length]
        self.index += length
        return ret.decode("utf-8")

    def _parse_varint(self):
        ret = 0
        shift = 0
        while True:
            b = self.byte_array[self.index]
            self.index += 1
            has_next = b & 0b10000000
            cur = b & 0b01111111
            ret += (cur << shift)
            if not has_next:
                break
            shift += 7
        return ret

    def to_proto(self):
        p = testpb.Simple()
        while self.index < len(self.byte_array):
            _type, field = self._parse_type_and_field_num()
            if _type == 'string':
                value = self._parse_string()
            elif _type == 'varint':
                value = self._parse_varint()
            else:
                print(_type)
                assert False
            if field == 1:
                p.name = value
            elif field == 2:
                p.id = value
            elif field == 3:
                p.tag.append(value)
            else:
                assert False
        return p


p = testpb.Simple(
    name="nice",
    id=13312342427,
    tag=["t1", "t2"]
)
serialized = p.SerializeToString()
print(serialized.hex())
pd = ProtoDeserialize(serialized)
p2 = pd.to_proto()
print(p2)
print(p == p2)

# person = testpb.Person()
# person.name = "sai"
# person.id = 1
# person.email = "sai@sai.com"
# person.phones.append(testpb.Person.PhoneNumber(number="123", type=testpb.Person.PHONE_TYPE_MOBILE))
# person.phones.append(testpb.Person.PhoneNumber(number="456", type=testpb.Person.PHONE_TYPE_MOBILE))
# print(person)
# personbin = person.SerializeToString()
# print(personbin.hex())
# pd = ProtoDeserialize(personbin)
# print(pd._parse_type_and_field_num())
# print(pd._parse_string())
# print(pd._parse_type_and_field_num())
# print(pd._parse_varint())
# print(pd._parse_type_and_field_num())
# print(pd._parse_string())
#
# print(pd.byte_array[pd.index:].hex())
# print(pd._parse_type_and_field_num())
# print(pd._parse_type_and_field_num())
# print(pd._parse_string())
#
# print(pd.byte_array[pd.index:].hex())
# print(pd._parse_type_and_field_num())
# print(pd._parse_string())


