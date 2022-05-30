package util

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
)

type RelPathType string

var UniqueMachineId = getUniqueMachineIdOrDie()

func (p RelPathType) String() string {
	return string(p)
}

func PanicIf(cond bool, msg string) {
	if cond {
		panic(msg)
	}
}

func PanicIfFalse(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

func PanicIfErr(err error, msg string) {
	if err != nil {
		panic(fmt.Sprintf("%v err=%v", msg, err))
	}
}

func getUniqueMachineIdOrDie() string {
	mid, err := GetUniqueMachineId()
	if err != nil {
		panic(fmt.Errorf("could not GetUniqueMachineId: %v", err))
	} else {
		log.Printf("getUniqueMachineIdOrDie: %v", mid)
	}
	return mid
}

func GetUniqueMachineId() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	// Sort the interfaces.
	sort.Slice(interfaces, func(i, j int) bool {
		return interfaces[i].Name < interfaces[j].Name
	})

	macAddress := ""
	if err == nil {
		for _, i := range interfaces {
			if i.Name == "en0" { // In mac en0 is the wifi network interface.
				macAddress = i.HardwareAddr.String()
				break
			}
			// Make sure that the interface has a hardware address.
			// Not clear to me if this is the "perfect" thing to do. But should be fine.
			// NOTE: Maybe i should check if the interface is up or not. Did not do it because
			// I want to make sure when this function is called twice, it will return the same
			// address
			if bytes.Compare(i.HardwareAddr, nil) != 0 {
				// Don't use random as we have a real address
				macAddress = i.HardwareAddr.String()
				break
			}
		}
	}
	if macAddress == "" {
		return "", errors.New("could not find mac address")
	}
	return base64.StdEncoding.EncodeToString(sha1.New().Sum([]byte(macAddress))), nil
}
