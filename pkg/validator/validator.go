package validator

import (
	"github.com/go-playground/validator/v10"
)

type Validator struct {
	Validate *validator.Validate
}

func NewValidator() *Validator {
	v := validator.New()
	return &Validator{
		Validate: v,
	}
}
