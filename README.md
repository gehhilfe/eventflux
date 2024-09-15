# EventFlux
EventFlux allows the creation of synchronized event stores using a message bus like NATS. With EventFlux, you can easily handle and manage events, enabling seamless communication between different components of your system.

Every store is independent but synchronizes contained events with other stores. This allows for the creation of local projections without the need to read data from a store connected via a message bus. The data is replicated to ensure synchronization.

## License

EventFlux is licensed under the [MIT License](https://github.com/eventflux/eventflux/blob/main/LICENSE).
