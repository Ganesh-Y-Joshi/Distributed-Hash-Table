package org.joshi.gyj.hnsw;

import org.joshi.gyj.nsqldb.db.util.datastrs.algorithms.MurmurHash;

import java.lang.reflect.Array;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;


/**
 * A utility class for consistent hashing.
 *
 * @param <T> The type of the elements to be hashed.
 */
public class ConsistentHashing<T> {

    /**
     * A class representing the hashing mechanism used in consistent hashing.
     *
     * @param <T> The type of the elements to be hashed.
     */
    public static class Hashing<T> {

        private final int maxSize;

        /**
         * Constructs a Hashing instance with the specified maximum size.
         *
         * @param maxSize The maximum size for hashing.
         */
        public Hashing(int maxSize) {
            this.maxSize = maxSize;
        }

        /**
         * Gets the index for the specified value in the hash space.
         *
         * @param value The value to be hashed.
         * @return The index in the hash space.
         */
        public int getIndex(T value) {
            return Math.abs(
                    MurmurHash.murmur3Hash32(value.toString(), 42) % maxSize
            );
        }
    }

    /**
     * Enumeration representing the current status of a node.
     */
    public enum CurrentStatus {
        NODE_WORKING {
            final String getMessage() {
                return "The given node is working";
            }
        },
        NODE_NOT_WORKING {
            final String getMessage() {
                return "The given node is not working";
            }
        }
    }

    /**
     * A class representing the ring structure used in consistent hashing.
     *
     * @param <T> The type of the elements to be stored in the ring.
     */
    public static class Ring<T> {
        private final T[] fixedArray;
        private final int maxValue;
        private final Hashing<T> hasher;

        /**
         * Constructs a Ring instance with the specified element type and maximum size.
         *
         * @param c       The class representing the element type.
         * @param maxSize The maximum size for the ring.
         */
        public Ring(Class<T> c, int maxSize) {
            if (maxSize <= 0) {
                throw new IllegalArgumentException("We cannot have a maxSize <= 0");
            }

            this.maxValue = maxSize;
            this.fixedArray = (T[]) Array.newInstance(c, maxSize);
            hasher = new Hashing<>(maxSize);
        }

        /**
         * Gets a snapshot of the elements in the ring.
         *
         * @return An array representing the elements in the ring.
         */
        public T[] getSnapshot() {
            return fixedArray;
        }

        /**
         * Adds an entry to the ring for the specified value.
         *
         * @param value The value to be added.
         * @return The index at which the value is added.
         */
        public int addEntry(T value) {
            if (value == null) {
                throw new NullPointerException("Nulls are not allowed");
            }

            int index = hasher.getIndex(value);
            if (index >= 0) {
                if (fixedArray[index] == null) {
                    fixedArray[index] = value;
                } else {
                    for (int i = index; i < maxValue; i++) {
                        if (fixedArray[i] == null) {
                            fixedArray[i] = value;
                            return i;
                        }
                    }

                    for (int i = 0; i < index; i++) {
                        if (fixedArray[i] == null) {
                            fixedArray[i] = value;
                            return i;
                        }
                    }
                }
            }
            return -1;
        }

        /**
         * Finds the entry index for the specified value in the ring.
         *
         * @param value The value to find.
         * @return The index of the entry if found; otherwise, -1.
         */
        public int findEntry(T value) {
            if (value == null) {
                throw new NullPointerException("Nulls are not allowed");
            }

            int index = hasher.getIndex(value);
            if (index >= 0) {
                if (fixedArray[index] == value) {
                    return index;
                } else {
                    int j = 0;
                    for (int i = index; i < maxValue; i++) {
                        if (fixedArray[i] != null) {
                            return i;
                        }
                    }

                    for (int i = 0; i < index; i++) {
                        if (fixedArray[i] != null) {
                            return i;
                        }
                    }
                    return -1;
                }
            } else {
                return -1;
            }
        }
    }

    /**
     * A class representing a store of nodes.
     *
     * @param <T> The type of the elements to be stored in the node.
     */
    public static class Store<T> {
        /**
         * A request to store an element.
         *
         * @param <T> The type of the element to be stored.
         */
        public static final class Request<T> {
            private final T data;
            private Request<T> request;

            /**
             * Constructs a Request instance with the specified data and request.
             *
             * @param data    The data to be stored.
             * @param request The request associated with the data.
             */
            public Request(T data, Request<T> request) {
                this.request = request;
                this.data = data;
            }

            /**
             * Gets the data associated with the request.
             *
             * @return The data.
             */
            public T getData() {
                return data;
            }

            /**
             * Gets the request associated with the data.
             *
             * @return The request.
             */
            public Request<T> getRequest() {
                return request;
            }

            /**
             * Sets the request associated with the data.
             *
             * @param request The request to be set.
             */
            public void setRequest(Request<T> request) {
                this.request = request;
            }

            /**
             * Checks if the current request is equal to another object.
             *
             * @param o The object to compare.
             * @return True if equal; otherwise, false.
             */
            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Request<?> request1 = (Request<?>) o;
                return Objects.equals(data, request1.data) && Objects.equals(request, request1.request);
            }

            /**
             * Computes the hash code for the request.
             *
             * @return The hash code.
             */
            @Override
            public int hashCode() {
                return Objects.hash(data, request);
            }
        }

        private final List<Request<T>> nodeList;

        /**
         * Constructs a Store instance.
         */
        public Store() {
            nodeList = new LinkedList<>();
        }

        /**
         * Gets the list of nodes in the store.
         *
         * @return The list of nodes.
         */
        public List<Request<T>> getNodeList() {
            return nodeList;
        }

        /**
         * Adds a node to the store.
         *
         * @param value The node to be added.
         * @return True if the node is added successfully; otherwise, false.
         */
        public boolean addNode(Request<T> value) {
            if (value == null) {
                throw new NullPointerException("value cannot be null");
            }

            return nodeList.add(value);
        }

        /**
         * Removes a node from the store.
         *
         * @param value The node to be removed.
         * @return True if the node is removed successfully; otherwise, false.
         */
        public boolean remove(Request<T> value) {
            if (value == null) {
                throw new NullPointerException("value cannot be null");
            }

            return nodeList.remove(value);
        }
    }

    private final Ring<T> ring;
    private final int maxValue;
    private final Hashing<T> hasher;

    /**
     * Constructs a ConsistentHashing instance with the specified element type and maximum size.
     *
     * @param c        The class representing the element type.
     * @param maxValue The maximum size for the consistent hashing.
     */
    public ConsistentHashing(Class<T> c, int maxValue) {
        if (maxValue <= 0) {
            throw new IllegalArgumentException("The max value should be >= 0");
        }

        this.maxValue = maxValue;
        ring = new Ring<>(c, maxValue);
        hasher = new Hashing<>(maxValue);
    }

    /**
     * Gets the ring used in consistent hashing.
     *
     * @return The ring.
     */
    public Ring<T> getRing() {
        return ring;
    }

    /**
     * Gets the maximum value for consistent hashing.
     *
     * @return The maximum value.
     */
    public int getMaxValue() {
        return maxValue;
    }

    /**
     * Adds a server entry point to the consistent hashing ring.
     *
     * @param value The value representing the server entry point.
     * @return The index at which the server entry point is added.
     */
    public int addServerEntryPoint(T value) {
        if (value == null) {
            throw new NullPointerException("The value cannot be null");
        }

        int index;
            index = ring.addEntry(value);
        return index;
    }

    /**
     * Finds the server mapping for the specified value in the consistent hashing ring.
     *
     * @param value The value for which to find the server mapping.
     * @return The server mapping for the value.
     */
    public T findServerMap(T value) {
        if (value == null) {
            throw new NullPointerException("The value cannot be null");
        }

            int index = hasher.getIndex(value);
            if (index > 0) {
                if (ring.fixedArray[index] != null) {
                    return ring.fixedArray[index];
                } else {

                    for (int i = index; i < maxValue; i++) {
                        if (ring.fixedArray[i] != null) {
                            return ring.fixedArray[i];
                        }
                    }

                    for (int i = 0; i < index; i++) {
                        if (ring.fixedArray[i] != null) {
                            return ring.fixedArray[i];
                        }
                    }

                    return null;
                }
            } else {
                return null;
            }
    }
}
