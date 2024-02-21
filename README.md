Consistent Hashing Utility
This utility provides a simple implementation of consistent hashing, a technique commonly used in distributed systems for efficiently distributing data across a set of nodes. The utility consists of two main classes:

MurmurHash: This class provides an implementation of the Murmur3 hash function, which is used for generating 32-bit hash values from input keys or byte arrays.

ConsistentHashing: This class implements the logic for consistent hashing and includes sub-classes for different components of the consistent hashing system.

Classes
ConsistentHashing: This is the main class that orchestrates the consistent hashing process. It includes methods for adding server entry points to the consistent hashing ring and finding the server mapping for a given value in the ring.

ConsistentHashing.Hashing: This nested class represents the hashing mechanism used in consistent hashing. It computes the index for a given value in the hash space.

ConsistentHashing.Ring: This nested class represents the ring structure used in consistent hashing. It maintains an array of elements where each element corresponds to a position in the hash space. The ring supports adding entries and finding entries based on a given value.

ConsistentHashing.Store: This nested class represents a store of nodes. It maintains a list of nodes and includes methods for adding and removing nodes from the store.
