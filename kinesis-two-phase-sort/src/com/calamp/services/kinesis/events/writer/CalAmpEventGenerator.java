/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.calamp.services.kinesis.events.writer;

import java.util.UUID;

import com.calamp.services.kinesis.events.data.CalAmpEvent;

/**
 * Generates random messages.
 *
 */

public class CalAmpEventGenerator {
    
    public CalAmpEvent getRandomMessage() {
    	String r1 = UUID.randomUUID().toString();
    	String r2 = UUID.randomUUID().toString();
    	String r3 = UUID.randomUUID().toString();
    	String r4 = UUID.randomUUID().toString();
        return new CalAmpEvent(r1, r2, r3, r4);
    }
}
