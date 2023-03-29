package util

func ArrayIntToInt32(arr []int) []int32 {

	newArr := make([]int32, len(arr))
	for i, v := range arr {
		newArr[i] = int32(v)
	}

	return newArr

}

func RemoveElementOnInt32Array(arr []int32, ele int32) []int32 {

	newArr := make([]int32, 0)
	for _, v := range arr {
		if v != ele {
			newArr = append(newArr, v)
		}
	}

	return newArr

}
