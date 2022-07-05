package gddd

import (
	"errors"
	"fmt"
)

func main() {
	err := errors.New("aa")
	fmt.Println(err.Error())
}
