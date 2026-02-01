# B-Tree Page Low-Level Design (LLD)

## Overview

This document describes the low-level design of the B-Tree page implementation for the Rusty-KV storage engine. The design implements a slotted page structure optimized for variable-length key-value pairs with efficient space management and binary search capabilities.

## Architecture

### Page Layout

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              PAGE (8KB)                                     │
├─────────────┬───────────────────────────────────────────────────────────────┤
│   HEADER    │                        BODY                                   │
│   (2 bytes) │                     (7998 bytes)                              │
└─────────────┴───────────────────────────────────────────────────────────────┘

BODY Layout:
┌─────────────┬─────────────┬─────────────────────────────────────────────────┐
│ ROW DATA    │ FREE SPACE  │              SLOT MAP                           │
│ (variable)  │ (variable)  │            (grows leftward)                     │
└─────────────┴─────────────┴─────────────────────────────────────────────────┘
```

### Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `PAGE_SIZE` | 8000 bytes | Total page size |
| `PAGE_HEADER_SIZE` | 2 bytes | Header size (slot count) |
| `PAGE_BODY_SIZE` | 7998 bytes | Body size (PAGE_SIZE - HEADER_SIZE) |
| `SLOT_MAP_ELEMENT_SIZE` | 2 bytes | Size of each slot map entry |
| `ROW_HEADER_SIZE` | 4 bytes | Row header (key_size + value_size) |

## Components

### 1. BTreePageHeader

**Purpose**: Manages page metadata stored in the first 2 bytes of the page.

**Structure**:
```rust
struct BTreePageHeader<'a> {
    data: &'a mut [u8], // 2-byte header
}
```

**Layout**:
```
Offset 0-1: slot_count (u16, little-endian)
```

**Key Methods**:
- `get_slot_count()` - Returns number of active slots
- `set_slot_count(count)` - Updates slot count
- `increase_slot_count(increment)` - Atomically increments slot count

**Invariants**:
- Slot count never decreases (only increases or stays same)
- Maximum slot count limited by available space

### 2. BTreePageSlotMap

**Purpose**: Manages the slot directory that maps logical slot indices to physical row offsets.

**Structure**:
```rust
struct BTreePageSlotMap {
    start: usize, // Offset where slot map begins
}
```

**Layout**: Grows leftward from end of page body
```
[... FREE SPACE ...][slot_n][slot_n-1][...][slot_1][slot_0]
                     ^
                     start
```

**Each slot entry**: 2 bytes (u16) containing row offset in little-endian format

**Key Methods**:
- `get_slot_map_element(index, data)` - Retrieves slot entry bytes
- `insert_slot_element(free_space, data, element, index)` - Inserts new slot entry

**Insertion Algorithm**:
1. Allocate space at end of free space (leftward growth)
2. Shift existing elements left to make space at insertion point
3. Insert new element at correct sorted position

### 3. BTreePageFreeSpace

**Purpose**: Manages the free space region between row data and slot map.

**Structure**:
```rust
struct BTreePageFreeSpace {
    start: usize, // Start of free space
    end: usize,   // End of free space
}
```

**Key Methods**:
- `allocate_row_space(size)` - Allocates space for new row (grows rightward)
- `allocate_slot_map_space()` - Allocates space for slot entry (grows leftward)
- `get_size()` - Returns available free space

**Space Management**:
- Row data grows rightward from `start`
- Slot map grows leftward from `end`
- Free space shrinks from both ends

### 4. BTreeBodyData

**Purpose**: Main controller for page body operations including CRUD operations.

**Structure**:
```rust
struct BTreeBodyData<'a> {
    data: &'a mut [u8],           // Page body bytes
    free_space: BTreePageFreeSpace, // Free space manager
    slot_map: BTreePageSlotMap,     // Slot map manager
}
```

**Initialization Algorithm** (`from` method):
1. Calculate slot map start position based on slot count
2. Traverse existing rows sequentially to find free space start
3. Initialize free space region between row data end and slot map start
4. Create slot map view starting from calculated position

**Key Operations**:

#### Search (`search` method)
- **Algorithm**: Binary search on slot map
- **Time Complexity**: O(log n)
- **Returns**: `Ok(index)` if found, `Err(insertion_index)` if not found

#### Get (`get` method)
1. Perform binary search to find slot index
2. Read row offset from slot map
3. Parse row header to get key/value sizes
4. Return complete row data

#### Insert (`insert` method)
1. Allocate space for row data in free space
2. Write row header (key_size, value_size)
3. Write key and value data
4. Insert row offset into slot map at sorted position
5. Update free space boundaries

#### Update (`update` method)
1. Locate existing row using slot map index
2. Validate new value fits in existing space
3. Overwrite value data in-place
4. Return updated row reference

### 5. Row Format

**Row Header** (4 bytes):
```
Offset 0-1: key_size (u16, little-endian)
Offset 2-3: value_size (u16, little-endian)
```

**Row Data Layout**:
```
[ROW_HEADER][KEY_DATA][VALUE_DATA]
    4 bytes   variable   variable
```

**Helper Module** (`btree_row`):
- `get_key_size(data)` / `set_key_size(data, size)`
- `get_value_size(data)` / `set_value_size(data, size)`
- `get_key(data)` / `set_key(data, key)`
- `get_value(data)` / `set_value(data, value)`

### 6. BTreePage

**Purpose**: High-level page interface combining header and body operations.

**Structure**:
```rust
struct BTreePage<'a> {
    body: BTreeBodyData<'a>,
    header: BTreePageHeader<'a>,
}
```

**Public API**:
- `get(key)` - Retrieve value by key
- `save(key, value)` - Insert or update key-value pair

**Save Algorithm**:
1. Search for existing key
2. If found: Update value in-place
3. If not found: Insert new row and increment slot count

## Key Design Decisions

### 1. Slotted Page Structure
- **Rationale**: Supports variable-length records efficiently
- **Benefits**: No external fragmentation, efficient space utilization
- **Trade-offs**: Slight overhead for slot directory

### 2. Sequential Row Storage
- **Rationale**: Simplifies free space calculation and page initialization
- **Benefits**: Predictable layout, easy compaction
- **Constraint**: Slot count never decreases (maintains sequential property)

### 3. Leftward-Growing Slot Map
- **Rationale**: Allows dynamic growth without moving existing data
- **Benefits**: Efficient insertion, no data movement during slot map expansion
- **Implementation**: Slot map grows from page end toward beginning

### 4. Binary Search on Slot Map
- **Rationale**: Maintains sorted order for efficient lookups
- **Benefits**: O(log n) search complexity
- **Requirement**: Keys must be comparable using little-endian byte ordering

### 5. In-Place Updates
- **Rationale**: Avoids data movement for same-size value updates
- **Benefits**: Better performance, reduced fragmentation
- **Limitation**: New value must match existing value size exactly

## Memory Safety

### Lifetime Management
- All views use Rust lifetimes to ensure memory safety
- Page header and body share the same underlying byte array
- No dangling references possible due to lifetime constraints

### Bounds Checking
- All array accesses protected by assertions
- Slot map access validates indices before dereferencing
- Free space allocation checks available space before proceeding

### Error Handling
- Current implementation uses assertions and `unwrap()`
- TODO: Replace with proper error types for production use

## Performance Characteristics

| Operation | Time Complexity | Space Complexity |
|-----------|----------------|------------------|
| Search | O(log n) | O(1) |
| Insert | O(log n) + O(k) | O(k) |
| Update | O(log n) | O(1) |
| Get | O(log n) | O(1) |

Where:
- n = number of rows in page
- k = size of key + value being inserted

## Limitations and Future Enhancements

### Current Limitations
1. No page compaction (fragmentation after deletions)
2. Fixed value size for updates
3. No concurrent access support
4. Error handling via panics instead of proper error types

### Planned Enhancements
1. Page compaction algorithm
2. Variable-size value updates with space reclamation
3. Concurrent access with page-level locking
4. Proper error handling with custom error types
5. Page corruption detection and recovery

## Testing Strategy

### Unit Tests
- Page header operations (get/set/increase slot count)
- Row helper functions (key/value size and data operations)
- Integration test with multiple insert/update operations

### Test Coverage
- Basic CRUD operations
- Edge cases (full page, empty page)
- Data persistence across page reconstructions
- Slot map ordering and binary search correctness

## Dependencies

### Internal Dependencies
- `commons::PAGE_SIZE` - Page size constant
- `constants::page_constants` - Row header layout constants
- `helpers::byte_ordering::cmp_le_bytes` - Little-endian byte comparison
- `helpers::row_helper::btree_row` - Row manipulation utilities

### External Dependencies
- `std::cmp::Ordering` - Comparison result type
- `std::mem::size_of` - Type size calculation

## Conclusion

This B-Tree page implementation provides a solid foundation for a key-value storage engine with efficient space utilization, logarithmic search performance, and memory-safe operations. The slotted page design with sequential row storage and leftward-growing slot map offers a good balance between performance and implementation complexity.
