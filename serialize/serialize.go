package serialize

type ToyCarSerializer interface {
	Serialize(interface{}) ([]byte, error)
}

type ToyCarDeserializer interface {
	Deserialize([]byte) (interface{}, error)
}

type SimpleStringSerializer struct {
}

type SimpleStringDeserializer struct {
}

func (ser *SimpleStringSerializer) Serialize(content interface{}) ([]byte, error) {

	return []byte(content.(string)), nil

}

func (ser *SimpleStringDeserializer) Deserialize(data []byte) (interface{}, error) {

	var i interface{} = string(data)
	return i.(string), nil

}

//func demo(s ToyCarSerializer, d ToyCarDeserializer) {
//	content := "hello"
//	bytes, _ := s.Serialize(content)
//	fmt.Println(bytes)
//
//	result, _ := d.Deserialize(bytes)
//	fmt.Println(result)
//}
//
//func main() {
//	serializer := &SimpleStringSerializer{}
//	d := &SimpleStringDeserializer{}
//	demo(serializer, d)
//}
