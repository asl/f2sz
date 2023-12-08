/* ******************************************************************
 * f2sz
 * Copyright (c) 2023, Anton Korobeynikov
 *
 * This source code is licensed under the GPLv3 (found in the LICENSE
 * file in the root directory of this source tree).
 ****************************************************************** */

#include "utils.h"

#include <string.h>
#include <stdbool.h>

bool strEndsWith(const char *str, const char *suf) {
  size_t strLen = strlen(str);
  size_t sufLen = strlen(suf);

  return (strLen >= sufLen) && (0 == strcmp(str + (strLen - sufLen), suf));
}

size_t decodeMultiplier(char *arg) {
  size_t multiplier = 1;
  if (strEndsWith(arg, "k") || strEndsWith(arg, "K") ||
      strEndsWith(arg, "KiB")) {
    multiplier = 1024;
  } else if (strEndsWith(arg, "M") || strEndsWith(arg, "MiB")) {
    multiplier = 1024 * 1024;
  } else if (strEndsWith(arg, "G") || strEndsWith(arg, "GiB")) {
    multiplier = 1024 * 1024 * 1024;
  } else if (strEndsWith(arg, "kB") || strEndsWith(arg, "KB")) {
    multiplier = 1000;
  } else if (strEndsWith(arg, "MB")) {
    multiplier = 1000 * 1000;
  } else if (strEndsWith(arg, "GB")) {
    multiplier = 1000 * 1000 * 1000;
  }
  return multiplier;
}
