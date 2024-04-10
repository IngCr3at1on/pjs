package util

type CalcF func(int) int

func HalfWidth(terminalWidth int) int {
	return terminalWidth / 2
}

func FullHeight(terminalHeight int) int {
	return terminalHeight
}
