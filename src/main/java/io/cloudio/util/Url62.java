/*
 * Copyright (c) 2014 - present CloudIO Inc.
 * 1248 Reamwood Ave, Sunnyvale, CA 94089
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * CloudIO Inc. ("Confidential Information").  You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with CloudIO.
 */

package io.cloudio.util;

import java.math.BigInteger;
import java.util.UUID;

class Url62 {

  /**
   * Encode UUID to Url62 id
   *
   * @param uuid
   *          UUID to be encoded
   * @return url62 encoded UUID
   */
  static String encode(UUID uuid) {
    BigInteger pair = UuidConverter.toBigInteger(uuid);
    return Base62.encode(pair);
  }

  /**
   * Decode url62 id to UUID
   *
   * @param id
   *          url62 encoded id
   * @return decoded UUID
   */
  static UUID decode(String id) {
    BigInteger decoded = Base62.decode(id);
    return UuidConverter.toUuid(decoded);
  }

}
