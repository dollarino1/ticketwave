package server

import (
	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	inventoryv1.UnimplementedInventoryServiceServer
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Server {
	return &Server{pool: pool}
}

func CreateEvent(req *inventoryv1.CreateEventRequest) (*inventoryv1.CreateEventResponse, error) {

	return &inventoryv1.CreateEventResponse{}, nil
}

func ListSeats(req *inventoryv1.ListSeatsRequest) (*inventoryv1.ListSeatsResponse, error) {

	return &inventoryv1.ListSeatsResponse{}, nil
}

func ReserveSeats(req *inventoryv1.ReserveSeatsRequest) (*inventoryv1.ReserveSeatsResponse, error) {

	return &inventoryv1.ReserveSeatsResponse{}, nil
}

func ConfirmSeats(req *inventoryv1.ConfirmSeatsRequest) (*inventoryv1.ConfirmSeatsResponse, error) {
	return &inventoryv1.ConfirmSeatsResponse{}, nil
}

func ReleaseSeats(req *inventoryv1.ReleaseSeatsRequest) (*inventoryv1.ReleaseSeatsResponse, error) {
	return &inventoryv1.ReleaseSeatsResponse{}, nil
}
