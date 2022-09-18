#pragma once

#if __GNUC__
#define FOLLY_DETAIL_BUILTIN_EXPECT(b, t) (__builtin_expect(b, t))
#else
#define FOLLY_DETAIL_BUILTIN_EXPECT(b, t) b
#endif

/**
 * Treat the condition as likely.
 *
 * @def FOLLY_LIKELY
 */
#define FOLLY_LIKELY(...) FOLLY_DETAIL_BUILTIN_EXPECT((__VA_ARGS__), 1)

/**
 * Treat the condition as unlikely.
 *
 * @def FOLLY_UNLIKELY
 */
#define FOLLY_UNLIKELY(...) FOLLY_DETAIL_BUILTIN_EXPECT((__VA_ARGS__), 0)

// Un-namespaced annotations

#undef LIKELY
#undef UNLIKELY

/**
 * Treat the condition as likely.
 *
 * @def LIKELY
 */
/**
 * Treat the condition as unlikely.
 *
 * @def UNLIKELY
 */
#if defined(__GNUC__)
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif