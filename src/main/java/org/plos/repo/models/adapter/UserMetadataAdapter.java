package org.plos.repo.models.adapter;

import org.plos.repo.service.RepoException;
import org.w3c.dom.*;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.HashMap;
import java.util.Map;

public class UserMetadataAdapter extends XmlAdapter<Element, Map<String, String>> {
    
    private DocumentBuilder documentBuilder;

    private String tagName;
    
    public UserMetadataAdapter() throws Exception {
        documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    }

    @Override
    public Element marshal(Map<String, String> map) throws Exception {
        final Document document = documentBuilder.newDocument();
        Element  rootElement = document.createElement("userMetadata");
        document.appendChild(rootElement);
        if(map != null) {
            try {
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    Element childElement = document.createElement(entry.getKey());
                    childElement.setTextContent(entry.getValue().toString());
                    rootElement.appendChild(childElement);
                }
            }catch(DOMException e){
                System.out.println("Error: " + e);
                rootElement = null;
            } catch(Exception e){
                System.out.println("Error: " + e);
                rootElement = null;
            }
        }
        //System.out.println("map " + map);
        //System.out.println("rootElement " + rootElement);
        return rootElement;
        }

    @Override
    public Map<String, String> unmarshal(Element rootElement) throws Exception {
        System.out.println("rootElement " + rootElement);
        if(rootElement == null)
            return null;
        NodeList nodeList = rootElement.getChildNodes();
        Map<String,String> map = new HashMap<String, String>(nodeList.getLength());
        for(int x=0; x<nodeList.getLength(); x++) {
            Node node = nodeList.item(x);
            if(node.getNodeType() == Node.ELEMENT_NODE) {
                map.put(node.getNodeName(), node.getTextContent());
            }
        }
        return map;
    }

    public String getTagName() {
        return tagName;
    }

    public void setTagName(String tagName){
        this.tagName = tagName;
    }
}
