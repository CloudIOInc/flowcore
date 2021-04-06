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

import java.util.UUID;

public class FriendlyId {

  /**
   * Create FriendlyId id
   *
   * @return Friendly Id encoded UUID
   */
  public static String createFriendlyId() {
    return Url62.encode(UUID.randomUUID());
  }

  /**
   * Encode UUID to FriendlyId id
   *
   * @param uuid
   *          UUID to be encoded
   * @return Friendly Id encoded UUID
   */
  public static String toFriendlyId(UUID uuid) {
    return Url62.encode(uuid);
  }

  /**
   * Decode Friendly Id to UUID
   *
   * @param friendlyId
   *          encoded UUID
   * @return decoded UUID
   */
  public static UUID toUuid(String friendlyId) {
    return Url62.decode(friendlyId);
  }

}
