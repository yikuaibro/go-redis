package com_test

import (
	"testing"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

func TestCom(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Com Suite")
}
