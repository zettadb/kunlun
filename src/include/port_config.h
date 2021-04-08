#ifndef PORT_CONFIG_INCLUDED
#define PORT_CONFIG_INCLUDED
#ifndef MY_ATTRIBUTE
#if defined(__GNUC__) || defined(__clang__)
#define MY_ATTRIBUTE(A) __attribute__(A)
#else
#define MY_ATTRIBUTE(A)
#endif
#endif


#if defined __GNUC__
# define ATTRIBUTE_FORMAT(style, m, n) MY_ATTRIBUTE((format(style, m, n)))
#else
# define ATTRIBUTE_FORMAT(style, m, n)
#endif

// TODO: check following features in configure and define them in 
#define TARGET_OS_LINUX 1
#define HAVE_SYS_PRCTL_H 1
#define BACKTRACE_DEMANGLE 1
#define HAVE_STACKTRACE 1
#define HAVE_WRITE_CORE 1
#define HAVE_BACKTRACE 1
#define HAVE_BACKTRACE_SYMBOLS 1
//#define HAVE_ABI_CXA_DEMANGLE 1
#define HAVE_EXECINFO_H 1
//HAVE_BFD_H does NOT exist!
//
//
#define FN_LIBCHAR '/'  // we don't support Windows anyway.
#define FN_REFLEN 512 // file path max len

#define STRING_WITH_LEN(fixed_str) (fixed_str),(sizeof((fixed_str)) - 1)

/*
 * was: my_stpmov.
  strcpy1(dst, src) moves all the  characters  of  src  (including  the
  closing NUL) to dst, and returns a pointer to the new closing NUL in
  dst.	 The similar UNIX routine strcpy returns the old value of dst,
  which I have never found useful.  strcpy1(strcpy1(dst,a),b) moves a//b
  into dst, which seems useful.
*/
static inline char *strcpy1(char *dst, const char *src) {
  while ((*dst++ = *src++))
	;
  return dst - 1;
}

/*
 * was: my_stpnmov
  strncpy1(dst,src,length) moves length characters, or until end, of src to
  dst and appends a closing NUL to dst if src is shorter than length.
  The result is a pointer to the first NUL in dst, or is dst+n if dst was
  truncated.
*/
static inline char *strncpy1(char *dst, const char *src, size_t n) {
  while (n-- != 0) {
	if (!(*dst++ = *src++)) return (char *)dst - 1;
  }
  return dst;
}

/**
 * was: my_stpcpy
   Copy a string from src to dst until (and including) terminating null byte.

   @param dst   Destination
   @param src   Source

   @note src and dst cannot overlap.
		 Use strcpy1() if src and dst overlaps.

   @note Unsafe, consider using my_stpnpy() instead.

   @return pointer to terminating null byte.
*/
static inline char *str_copy(char *dst, const char *src) {
#if defined(HAVE_BUILTIN_STPCPY)
  return __builtin_stpcpy(dst, src);
#elif defined(HAVE_STPCPY)
  return stpcpy(dst, src);
#else
  /* Fallback to implementation supporting overlap. */
  return strcpy1(dst, src);
#endif
}

/**
 * was: my_stpncpy
   Copy fixed-size string from src to dst.

   @param dst   Destination
   @param src   Source
   @param n     Maximum number of characters to copy.

   @note src and dst cannot overlap
		 Use strncpy1() if src and dst overlaps.

   @return pointer to terminating null byte.
*/
static inline char *str_ncopy(char *dst, const char *src, size_t n) {
#if defined(HAVE_STPNCPY)
  return stpncpy(dst, src, n);
#else
  /* Fallback to implementation supporting overlap. */
  return strncpy1(dst, src, n);
#endif
}

#endif // PORT_CONFIG_INCLUDED
