package org.mitre.cdc.sdp;

import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import com.caucho.hessian.client.HessianProxyFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import gov.cdc.vocab.service.VocabService;
import gov.cdc.vocab.service.bean.CodeSystem;
import gov.cdc.vocab.service.bean.CodeSystemConcept;
import gov.cdc.vocab.service.bean.ValueSet;
import gov.cdc.vocab.service.bean.ValueSetVersion;
import gov.cdc.vocab.service.dto.output.CodeSystemConceptResultDto;
import gov.cdc.vocab.service.dto.output.ValueSetConceptResultDto;

public class PhinSync {

    private VocabService service;
    private RestClient restClient;
    private String esSearchUrl;
    
    public PhinSync() {
        this.esSearchUrl = "http://127.0.0.1:9200";
    }

    public PhinSync(String esSerachUrl) {
        this.esSearchUrl = esSerachUrl;
    }

    private VocabService getService() {
        if (service != null) {
            return service;
        }
        HessianProxyFactory factory = new HessianProxyFactory();
        try {
            service = (VocabService) factory.create(VocabService.class,
                    "http://phinvads.cdc.gov/vocabService/v2");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return service;
    }

    
    private RestClient getESClient() throws Exception{
        if (restClient != null) {
            return restClient;
        }
        URL url = new URL(this.esSearchUrl);
        restClient = RestClient.builder(
    	        new HttpHost(url.getHost(), url.getPort(), url.getProtocol())).build();
        return restClient;
    }
    

    private void syncCodeSystemCodes(String oid) throws Exception {
        System.out.println("Syncing CodeSytemCodes " + oid);
        int page = 1;
        int perPage = 1000;
        int returned = 0;
        CodeSystemConceptResultDto result = null;
        do {
            result = getService().getCodeSystemConceptsByCodeSystemOid(oid,
                    page, perPage);
            System.out.println(" page" + page + " of "
                    + Math.ceil(result.getTotalResults() / perPage));
            List<CodeSystemConcept> codes = result.getCodeSystemConcepts();
            returned = codes.size();
            for (Iterator<CodeSystemConcept> iterator = codes.iterator(); iterator.hasNext();) {
                CodeSystemConcept codeSystemConcept = (CodeSystemConcept) iterator
                        .next();
                updateElasticSearch("codes",
                        codeSystemConcept.getCodeSystemOid(),
                        codeSystemConcept.getId(),
                        objectToJSON(codeSystemConcept));
            }
            page++;
        } while (returned == perPage);
    }

    private void syncValueSetCodes(String versionId) {
        int page = 1;
        int perPage = 1000;
        int returned = 0;

        ValueSetConceptResultDto result = null; // =
                                                // getService().getValueSetConceptsByValueSetVersionId(versionId,
                                                // page, perPage);

        do {
            result = getService().getValueSetConceptsByValueSetVersionId(
                    versionId, page, perPage);
            List codes = result.getValueSetConcepts();
            returned = codes.size();

            page++;
        } while (returned == perPage);

    }

    private void syncCodeSystems() throws Exception {
        List<CodeSystem> codeSystems = getService().getAllCodeSystems()
                .getCodeSystems();
        for (Iterator iterator = codeSystems.iterator(); iterator.hasNext();) {
            CodeSystem codeSystem = (CodeSystem) iterator.next();
            System.out
                    .println("Processing Code System " + codeSystem.getName());
            updateElasticSearch("code_systems", "code_system",
                    codeSystem.getOid(), objectToJSON(codeSystem));
            syncCodeSystemCodes(codeSystem.getOid());
        }
    }

    private void syncValueSetVersions() throws Exception {
        List<ValueSetVersion> vsv = getService().getAllValueSetVersions()
                .getValueSetVersions();
        for (Iterator iterator = vsv.iterator(); iterator.hasNext();) {
            ValueSetVersion valueSetVersion = (ValueSetVersion) iterator.next();
            System.out.println("Processing Valueset "
                    + valueSetVersion.getValueSetOid() + " "
                    + valueSetVersion.getVersionNumber());
            updateElasticSearch("value_sets", "value_set",
                    valueSetVersion.getId(), objectToJSON(valueSetVersion));
        }
    }

    private void syncValueSets() throws Exception {
        List<ValueSet> vsv = getService().getAllValueSets().getValueSets();
        for (Iterator iterator = vsv.iterator(); iterator.hasNext();) {
            ValueSet valueSet = (ValueSet) iterator.next();
            System.out.println("Processing Valueset " + valueSet.getName());
            updateElasticSearch("value_sets", "value_set", valueSet.getId(),
                    objectToJSON(valueSet));
        }
    }

    private String objectToJSON(Object obj) throws IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
        Gson gson = new GsonBuilder().create();
        return gson.toJson(obj);

    }

    private Connection getDbConnection() {

        try {
            return java.sql.DriverManager.getConnection("");
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    private void updateResponseSetDB(List<ValueSetVersion> versions)
            throws Exception {
        Connection conn = getDbConnection();
        if (conn == null) {
            return;
        }
        for (Iterator iterator = versions.iterator(); iterator.hasNext();) {
            ValueSetVersion valueSetVersion = (ValueSetVersion) iterator.next();
            PreparedStatement stmt = conn
                    .prepareStatement("Select count(*) from response_sets where oid=? and version=? ");
            stmt.setString(0, valueSetVersion.getValueSetOid());
            stmt.setLong(1, valueSetVersion.getVersionNumber());
            ResultSet res = stmt.executeQuery();
            stmt.close();
            int count = res.getInt("count");
            if (count == 0) {
                PreparedStatement insert = conn
                        .prepareStatement("Insert into response_sets "
                                + "(source,oid,version_independent_id, version, status, name, description,created_at"
                                + "values (?,?,?,?,?,?,?)");
                insert.setString(0, "PHIN_VADS");
                insert.setString(1, valueSetVersion.getValueSetOid());
                insert.setString(2, valueSetVersion.getValueSetOid());
                insert.setInt(3, valueSetVersion.getVersionNumber());
                insert.setString(4, valueSetVersion.getStatus());
                insert.setString(6, valueSetVersion.getDescription());
                insert.setTimestamp(7,
                        new java.sql.Timestamp(System.currentTimeMillis()));
                insert.executeUpdate();
                insert.close();
            }
        }
        conn.close();
    }

    private void updateElasticSearch(String index, String type, String id,
            String data) throws Exception {
    	

    	
        URL url = new URL(this.esSearchUrl + "/" + index + "/" + type + "/"
                + id + "/_update");

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setRequestProperty("Content-Type", "application/json");

        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestMethod("POST");
        String insert = "{" + "\"doc\": " + data + ",\"doc_as_upsert\": true}";
     
        OutputStream os = conn.getOutputStream();
        os.write(insert.getBytes("UTF-8"));
        os.close();
        conn.getInputStream().close();

    }

    public static void main(String[] args) throws Exception {

        PhinSync sync = new PhinSync();
        sync.syncCodeSystems();
        sync.syncValueSets();
        sync.syncValueSetVersions();
    }
}
