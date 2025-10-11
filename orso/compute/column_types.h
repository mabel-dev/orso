// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef ORSO_COLUMN_TYPES_H
#define ORSO_COLUMN_TYPES_H

#include <cstdint>
#include <vector>
#include <memory>
#include <stdexcept>

namespace orso {

// Physical types supported by the column store
enum class PhysicalType {
    BIT8,      // 8-bit integer
    BIT16,     // 16-bit integer  
    BIT32,     // 32-bit integer
    BIT64,     // 64-bit integer
    FIXED_WIDTH_ARRAY,  // Fixed-width array
    VARIABLE_WIDTH      // Variable-width (e.g., strings)
};

// RLE (Run-Length Encoding) Column implementation
template<typename T>
class RLEColumn {
private:
    std::vector<T> values_;      // Unique values in the run
    std::vector<uint32_t> lengths_;  // Length of each run
    
public:
    RLEColumn() = default;
    
    // Encode from raw values
    void encode(const T* data, size_t size);
    
    // Decode to raw values
    std::vector<T> decode() const;
    
    // Get encoded data
    const std::vector<T>& values() const { return values_; }
    const std::vector<uint32_t>& lengths() const { return lengths_; }
    
    // Get total number of elements when decoded
    size_t decoded_size() const;
};

// Dictionary Encoding Column implementation
template<typename T>
class DictionaryColumn {
private:
    std::vector<T> dictionary_;      // Unique values
    std::vector<uint32_t> indices_;  // Indices into dictionary
    
public:
    DictionaryColumn() = default;
    
    // Encode from raw values
    void encode(const T* data, size_t size);
    
    // Decode to raw values
    std::vector<T> decode() const;
    
    // Get encoded data
    const std::vector<T>& dictionary() const { return dictionary_; }
    const std::vector<uint32_t>& indices() const { return indices_; }
    
    // Get total number of elements
    size_t size() const { return indices_.size(); }
};

// Template implementations

template<typename T>
void RLEColumn<T>::encode(const T* data, size_t size) {
    if (size == 0) {
        return;
    }
    
    values_.clear();
    lengths_.clear();
    
    T current_value = data[0];
    uint32_t current_length = 1;
    
    for (size_t i = 1; i < size; ++i) {
        if (data[i] == current_value) {
            current_length++;
        } else {
            values_.push_back(current_value);
            lengths_.push_back(current_length);
            current_value = data[i];
            current_length = 1;
        }
    }
    
    // Add the last run
    values_.push_back(current_value);
    lengths_.push_back(current_length);
}

template<typename T>
std::vector<T> RLEColumn<T>::decode() const {
    std::vector<T> result;
    result.reserve(decoded_size());
    
    for (size_t i = 0; i < values_.size(); ++i) {
        for (uint32_t j = 0; j < lengths_[i]; ++j) {
            result.push_back(values_[i]);
        }
    }
    
    return result;
}

template<typename T>
size_t RLEColumn<T>::decoded_size() const {
    size_t total = 0;
    for (auto len : lengths_) {
        total += len;
    }
    return total;
}

template<typename T>
void DictionaryColumn<T>::encode(const T* data, size_t size) {
    if (size == 0) {
        return;
    }
    
    dictionary_.clear();
    indices_.clear();
    indices_.reserve(size);
    
    // Build dictionary and indices
    std::vector<T> unique_values;
    for (size_t i = 0; i < size; ++i) {
        T value = data[i];
        
        // Find value in unique_values
        auto it = std::find(unique_values.begin(), unique_values.end(), value);
        if (it == unique_values.end()) {
            // New unique value
            unique_values.push_back(value);
            indices_.push_back(unique_values.size() - 1);
        } else {
            // Existing value
            indices_.push_back(std::distance(unique_values.begin(), it));
        }
    }
    
    dictionary_ = std::move(unique_values);
}

template<typename T>
std::vector<T> DictionaryColumn<T>::decode() const {
    std::vector<T> result;
    result.reserve(indices_.size());
    
    for (auto idx : indices_) {
        if (idx >= dictionary_.size()) {
            throw std::out_of_range("Dictionary index out of range");
        }
        result.push_back(dictionary_[idx]);
    }
    
    return result;
}

} // namespace orso

#endif // ORSO_COLUMN_TYPES_H
