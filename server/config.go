package main

type Config struct {
	IsCoordinator bool
	Addr          string
	Port          uint16
	Coordinators  []string
}
