package config

type Config struct {
	Segment struct {
		Store struct {
			MaxBytes uint64
		}
		Index struct {
			MaxBytes uint64
		}
	}
}
