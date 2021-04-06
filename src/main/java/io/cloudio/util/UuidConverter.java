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

class UuidConverter {

  static BigInteger toBigInteger(UUID uuid) {
    return BigIntegerPairing.pair(
        BigInteger.valueOf(uuid.getMostSignificantBits()),
        BigInteger.valueOf(uuid.getLeastSignificantBits()));
  }

  static UUID toUuid(BigInteger value) {
    BigInteger[] unpaired = BigIntegerPairing.unpair(value);
    return new UUID(unpaired[0].longValueExact(), unpaired[1].longValueExact());
  }

}
