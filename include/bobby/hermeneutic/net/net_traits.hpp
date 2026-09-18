#pragma once

#include <boost/asio/ssl.hpp>

#include <type_traits>

namespace bobby::hermeneutic::ingestion::detail {

// True for boost::asio::ssl::stream<T> (any T), false otherwise. Shared by
// WebSocketConnection and http_get so both can branch, at compile time, on
// whether their NextLayer needs an SNI + TLS handshake step.
template <typename T>
struct is_ssl_stream : std::false_type {};
template <typename T>
struct is_ssl_stream<boost::asio::ssl::stream<T>> : std::true_type {};
template <typename T>
inline constexpr bool is_ssl_stream_v = is_ssl_stream<T>::value;

}  // namespace bobby::hermeneutic::ingestion::detail
