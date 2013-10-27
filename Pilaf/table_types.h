#ifndef TABLE_TYPES_H
#define TABLE_TYPES_H

#define KVPT_SIZET_DOUBLE 0
#define KVPT_CHARP_CHARP  1

#define KEY_VAL_PAIRTYPE KVPT_CHARP_CHARP
//#define KEY_VAL_PAIRTYPE KVPT_SIZET_DOUBLE

#if KEY_VAL_PAIRTYPE==KVPT_SIZE_T_DOUBLE

#define KEY_TYPE size_t
#define VAL_TYPE double

#elif KEY_VAL_PAIRTYPE==KVPT_CHARP_CHARP

#define KEY_TYPE char*
#define VAL_TYPE char*

#endif

#endif // TABLE_TYPES_H
