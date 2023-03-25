package util

func ArrayIntToInt32(arr []int) []int32 {

	newArr := make([]int32, len(arr))
	for i, v := range arr {
		newArr[i] = int32(v)
	}

	return newArr

}
