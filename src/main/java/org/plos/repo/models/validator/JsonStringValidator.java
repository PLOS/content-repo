/*
 * Copyright (c) 2006-2015 by Public Library of Science
 *
 *    http://plos.org
 *    http://ambraproject.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.plos.repo.models.validator;

import com.google.gson.Gson;
import org.plos.repo.service.RepoException;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * Json String validator.
 */
public class JsonStringValidator {

  private Gson gson = new Gson();

  /**
   * Verifies if the <code>jsonString</code> is a valid json.
   * @param jsonString a single String representing a json to be validated
   * @param errorType RepoException.Type to be thrown if the String is not a valid json
   * @throws org.plos.repo.service.RepoException
   */
  public void validate(String jsonString, RepoException.Type errorType) throws RepoException {
    try {
      gson.fromJson(jsonString, Object.class);
    } catch(com.google.gson.JsonSyntaxException ex) {
      throw new RepoException(errorType);
    }
  }

  /**
   * Verifies if the <code>jsonString</code> is a valid json.
   * @param jsonString a single String representing a json to be validated
   * @param errorType RepoException.Type to be thrown if the String is not a valid json
   * @throws org.plos.repo.service.RepoException
   */
  public void validate(Map<String, String> json, RepoException.Type errorType) throws RepoException {
    try {
      if(json != null) {
        System.out.print("***** Map " + json);
        String result = gson.toJson(json);
        System.out.print("***** Result " + result);
      }
    } catch(com.google.gson.JsonSyntaxException ex) {
      throw new RepoException(errorType);
    }
  }
  
  public String toJson(Map<String, String> userMetadata) throws RepoException {
    String json = null;
    try {
      if(userMetadata != null){
        json = gson.toJson(userMetadata);
      }
    } catch(com.google.gson.JsonSyntaxException ex) {
      throw new RepoException(ex);
    }
    return json;
  }

  public Map<String, String> toMap(String json) throws RepoException {
    Map<String, String> userMetadata = null;
    try {
      if(json != null){
        userMetadata = gson.fromJson(json, HashMap.class);
      }
    } catch(com.google.gson.JsonSyntaxException ex) {
      throw new RepoException(ex);
    }
    return userMetadata;
  }
}
