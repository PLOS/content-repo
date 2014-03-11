<%--
  Copyright (c) 2014 by Public Library of Science
  http://plos.org

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
--%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="en" xml:lang="en">
<head>
  <title>PLoS Content Repository REST API</title>
</head>
<body>

<h1>PLoS Content Repository REST API</h1>

<h2>Database contents</h2>
<c:forEach items="${counts}" var="count" >
    ${count.key}: ${count.value} <br />
</c:forEach>

<h2>Service info</h2>
<c:forEach items="${service}" var="info" >
    ${info.key}: ${info.value} <br />
</c:forEach>

<h2>API Actions</h2>
<c:forEach items="${handlerMethods}" var="entry">
    <c:if test="${not empty entry.key.patternsCondition.patterns}">
        ${entry.key.methodsCondition.methods}	${entry.key.patternsCondition.patterns}<br />
    </c:if>
</c:forEach>

<%--<h2>Some sample inputs</h2>--%>

<%--<ul>--%>
  <%--<li><a href="/buckets">/buckets</a></li>--%>
<%--</ul>--%>

<%--<hr/>--%>
<%--<center><img src="resources/squirrel.jpg" alt="A squirrel (they are scatter hoarders)" width="300"/></center>--%>

<%-- or perhaps an acorn woodpecker? --%>

</body>
</html>
