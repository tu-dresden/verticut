#ifndef INTEGRITY_H
#define INTEGRITY_H
#include <tr1/functional>

class Integrity64 {

#define BIG_CONSTANT(x) (x##LLU)

  private:
    // CRC variables/constants
    #define poly 0x42F0E1EBA9EA3693
    #define hvinit 0x0060034000F0D50B
    uint64_t crc_table[256];
	uint64_t seeds[4];
  public:

    Integrity64() {
      // Initialize the 256-entry CRC table
      for(int i=0; i<256; i++) {
        uint64_t part = (uint64_t)i;
        uint64_t hv = 0L;
        for(int j=0; j<8; j++, (part <<= 1)) {
          hv <<= 1;
          if (part & 0x80)
            hv ^= poly;
        }
        crc_table[i] = hv;
      }

      // Initialize the hash seeds
      seeds[0] = 0x199999999999997F;
      seeds[1] = 0x1999999999999990;
      seeds[2] = 0x01000193;
      seeds[3] = 0x0100019D;

    }

    // Calculate the 64-bit hash
    uint64_t crc(const char* msg, size_t len) {
      uint64_t output = hvinit;
      const uint8_t* msg_ = (uint8_t*)msg;
      for(size_t i=0; i<len; i++) {
        output = crc_table[(uint8_t)(output ^ msg_[i])] ^ (output >> 8);
      }
      return output;
    }

    uint64_t hashN(const void * key, size_t key_len, int n = 0) {
      return hash(key,seeds[n],key_len);
    }

    // This is the Murmur Hash
    uint64_t hash(const void * key, uint64_t seed, int len) {

      const uint64_t m = BIG_CONSTANT(0xc6a4a7935bd1e995);
      const int r = 47;

      uint64_t h = seed ^ (len * m);

      const uint64_t * data = (const uint64_t *)key;
      const uint64_t * end = data + (len/8);

      while(data != end)
      {
        uint64_t k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
      }

      const unsigned char * data2 = (const unsigned char*)data;

      switch(len & 7)
      {
      case 7: h ^= uint64_t(data2[6]) << 48;
      case 6: h ^= uint64_t(data2[5]) << 40;
      case 5: h ^= uint64_t(data2[4]) << 32;
      case 4: h ^= uint64_t(data2[3]) << 24;
      case 3: h ^= uint64_t(data2[2]) << 16;
      case 2: h ^= uint64_t(data2[1]) << 8;
      case 1: h ^= uint64_t(data2[0]);
              h *= m;
      };

      h ^= h >> r;
      h *= m;
      h ^= h >> r;

      return h;
    }

};

#endif // INTEGRITY_H
