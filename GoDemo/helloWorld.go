package main

import (
	"fmt"
	"math"
)

type Point struct{
	x float32
	y float32
}
func len(m *Point) float64{
	return math.Sqrt(float64(m.x*m.x + m.y*m.y))
}
func main() {
	point := Point{1.2,1.44}
	fmt.Println(len(&point))
}
