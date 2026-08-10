package main

import (
	"context"
	"fmt"
	"strings"
	"sync"

	inventoryv1 "github.com/dollarino1/ticketwave/gen/ticketwave/inventory/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer func() { _ = conn.Close() }()

	client := inventoryv1.NewInventoryServiceClient(conn)
	ctx := context.Background()

	eventResp, err := client.CreateEvent(ctx, &inventoryv1.CreateEventRequest{
		Name:      "Stress Test Event",
		SeatCount: 1,
	})
	if err != nil {
		panic(err)
	}
	eventID := eventResp.EventId
	fmt.Printf("created event %s with 1 seat\n", eventID)

	seatsResp, err := client.ListSeats(ctx, &inventoryv1.ListSeatsRequest{EventId: eventID})
	if err != nil {
		panic(err)
	}
	seatID := seatsResp.Seats[0].Id
	fmt.Printf("targeting seat %s\n", seatID)

	const concurrency = 20
	var wg sync.WaitGroup
	results := make(chan string, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			orderID := uuid.New().String()
			_, err := client.ReserveSeats(ctx, &inventoryv1.ReserveSeatsRequest{
				EventId: eventID,
				SeatIds: []string{seatID},
				OrderId: orderID,
			})
			if err != nil {
				results <- fmt.Sprintf("goroutine %2d: lost (%v)", n, err)
				return
			}
			results <- fmt.Sprintf("goroutine %2d: WON  (order %s)", n, orderID)
		}(i)
	}

	wg.Wait()
	close(results)

	won := 0
	for r := range results {
		fmt.Println(r)
		if strings.Contains(r, "WON") {
			won++
		}
	}

	fmt.Printf("\nwinners: %d (expected: 1)\n", won)
}
