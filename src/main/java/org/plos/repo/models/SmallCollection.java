/*
 * Copyright (c) 2006-2014 by Public Library of Science
 * http://plos.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.plos.repo.models;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

/**
 * Collection of objects.
 */
@XmlRootElement
public class SmallCollection {

    private String key; // what the user specifies
    @XmlJavaTypeAdapter(TimestampAdapter.class)
    private Timestamp timestamp;   // created time
    private String timestampString;   // created time
    private String bucketName;
    private String create;
    private List<SmallObject> objects;

    // empty constructor required for JAXB mapping
    public SmallCollection() {
    }

    public SmallCollection(Integer id, String key, Timestamp timestamp, String bucketName, Integer versionNumber) {
        this.key = key;
        this.timestamp = timestamp;
        this.bucketName = bucketName;
    }

    public String getKey() {
        return key;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getCreate() {
        return create;
    }

    public List<SmallObject> getObjects() {
        return objects;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public void setCreate(String create) {
        this.create = create;
    }

    public void setObjects(List<SmallObject> objects) {
        this.objects = objects;
    }

    public String getTimestampString() {
        return timestampString;
    }

    public void setTimestampString(String timestampString) {
        this.timestampString = timestampString;
    }
}
