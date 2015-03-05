package org.plos.repo.models.adapter;

import org.plos.repo.service.RepoException;
import org.w3c.dom.Element;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.DOMException;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Adapter manages the userMetadata in JAXB serialization
 */
public class UserMetadataAdapter extends XmlAdapter<Element, Map<String, String>> {
    
    private static final Logger log = LoggerFactory.getLogger(UserMetadataAdapter.class);

    private static final String TAG_NAME = "userMetadata";

    private DocumentBuilder documentBuilder;

    private String tagName;
    
    public UserMetadataAdapter() throws Exception {
        documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    }

    @Override
    public Element marshal(Map<String, String> map) throws Exception {
        if(map == null) {
            return null;
        }
        final Document document = documentBuilder.newDocument();
        Element rootElement = document.createElement(TAG_NAME);
        document.appendChild(rootElement);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            Element childElement = document.createElement(entry.getKey());
        if (entry.getValue() != null) {
            childElement.setTextContent(entry.getValue().toString());
        }
            rootElement.appendChild(childElement);
        }
        return rootElement;
    }

    @Override
    public Map<String, String> unmarshal(Element rootElement) throws Exception {
        Map<String,String> map = new HashMap<String, String>();
        if(rootElement != null) {
        NodeList nodeList = rootElement.getChildNodes();
        for(int x=0; x<nodeList.getLength(); x++) {
            Node node = nodeList.item(x);
            if(node.getNodeType() == Node.ELEMENT_NODE) {
                map.put(node.getNodeName(), node.getTextContent());
            }
        }
        }
        return map;
    }

}
