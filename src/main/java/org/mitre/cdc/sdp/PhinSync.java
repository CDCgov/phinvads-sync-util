package org.mitre.cdc.sdp;

import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.caucho.hessian.client.HessianProxyFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import gov.cdc.vocab.service.VocabService;
import gov.cdc.vocab.service.bean.CodeSystem;
import gov.cdc.vocab.service.bean.CodeSystemConcept;
import gov.cdc.vocab.service.bean.ValueSet;
import gov.cdc.vocab.service.bean.ValueSetVersion;
import gov.cdc.vocab.service.dto.output.CodeSystemConceptResultDto;

public class PhinSync {
	private Log logger=LogFactory.getLog(PhinSync.class);
	private VocabService service;
	private String esSearchUrl;

	public PhinSync() {
		this.esSearchUrl = "http://127.0.0.1:9200";
	}

	public PhinSync(String esSerachUrl) {
		this.esSearchUrl = esSerachUrl;
	}

	private VocabService getService() {
		logger.debug("Getting hessian client");
		if (service != null) {
			logger.debug("Returning already instantiated hessian client");
			return service;
		}
		logger.debug("Building hessian client");
		HessianProxyFactory factory = new HessianProxyFactory();
		try {
			service = (VocabService) factory.create(VocabService.class, "http://phinvads.cdc.gov/vocabService/v2");
		} catch (MalformedURLException e) {
			logger.debug("Error creating client", e);
		}
		logger.debug("returning hessian client");
		return service;
	}

	private void syncCodeSystemCodes(String oid) throws Exception {
		System.out.println("CODE SYSTEMS CODES");
		logger.info("Syncing CodeSytemCodesfor " + oid);
		int page = 1;
		int perPage = 1000;
		int returned = 0;
		CodeSystemConceptResultDto result = null;
		do {
			result = getService().getCodeSystemConceptsByCodeSystemOid(oid, page, perPage);
			logger.debug(" page" + page + " of " + Math.ceil(result.getTotalResults() / perPage));
			List<CodeSystemConcept> codes = result.getCodeSystemConcepts();
			returned = codes.size();
			for (Iterator<CodeSystemConcept> iterator = codes.iterator(); iterator.hasNext();) {
				CodeSystemConcept codeSystemConcept = (CodeSystemConcept) iterator.next();
				updateElasticSearch("codes", codeSystemConcept.getCodeSystemOid(), codeSystemConcept.getId(),
						objectToJSON(codeSystemConcept));
			}
			page++;
		} while (returned == perPage);
	}

	private void syncCodeSystems() throws Exception {
		System.out.println("CODE SYSTEMS");
		logger.debug("Syncing Code Systems");
		List<CodeSystem> codeSystems = getService().getAllCodeSystems().getCodeSystems();
		for (Iterator<CodeSystem> iterator = codeSystems.iterator(); iterator.hasNext();) {
			CodeSystem codeSystem = iterator.next();
			logger.info("Processing Code System " + codeSystem.getName());
			updateElasticSearch("code_systems", "code_system", codeSystem.getOid(), objectToJSON(codeSystem));
			syncCodeSystemCodes(codeSystem.getOid());
		}
	}

	private void syncValueSetVersions() throws Exception {
		logger.debug("Syncing Valueset versions");
		List<ValueSetVersion> vsv = getService().getAllValueSetVersions().getValueSetVersions();
		for (Iterator<ValueSetVersion> iterator = vsv.iterator(); iterator.hasNext();) {
			ValueSetVersion valueSetVersion = iterator.next();
			logger.info("Processing Valueset " + valueSetVersion.getValueSetOid() + " "
					+ valueSetVersion.getVersionNumber());
			updateElasticSearch("value_sets", "value_set", valueSetVersion.getId(), objectToJSON(valueSetVersion));
		}
	}

	private void syncValueSets() throws Exception {
		logger.debug("Syncing Valuesets");
		List<ValueSet> vsv = getService().getAllValueSets().getValueSets();
		for (Iterator<ValueSet> iterator = vsv.iterator(); iterator.hasNext();) {
			ValueSet valueSet =  iterator.next();
			logger.info("Processing Valueset " + valueSet.getName());
			updateElasticSearch("value_sets", "value_set", valueSet.getId(), objectToJSON(valueSet));
		}
	}

	private String objectToJSON(Object obj)
			throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Gson gson = new GsonBuilder().create();
		return gson.toJson(obj);

	}

	private void updateElasticSearch(String index, String type, String id, String data) throws Exception {

		URL url = new URL(this.esSearchUrl + "/" + index + "/" + type + "/" + id + "/_update");

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
		
		PhinSync sync = args.length == 0 ? new PhinSync() : new PhinSync(args[0]);
		sync.syncCodeSystems();
		sync.syncValueSets();
		sync.syncValueSetVersions();
	}
}
