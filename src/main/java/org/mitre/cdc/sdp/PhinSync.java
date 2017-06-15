package org.mitre.cdc.sdp;

import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

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
	private Logger logger = Logger.getLogger(PhinSync.class);
	private VocabService service;
	private String esSearchUrl;
	private String phinVadsUrl;

	public PhinSync() {
		this.esSearchUrl = "http://127.0.0.1:9200";
		this.phinVadsUrl = "https://phinvads.cdc.gov/vocabService/v2";
	}

	public PhinSync(String esSerachUrl) {
		this.esSearchUrl = esSerachUrl;
		this.phinVadsUrl = "https://phinvads.cdc.gov/vocabService/v2";
	}

	public PhinSync(String esSerachUrl, String phinVadsUrl) {
		this.esSearchUrl = esSerachUrl;
		this.phinVadsUrl = phinVadsUrl;
	}

	public String getEsSearchUrl() {
		return esSearchUrl;
	}

	public void setEsSearchUrl(String esSearchUrl) {
		this.esSearchUrl = esSearchUrl;
	}

	public String getPhinVadsUrl() {
		return phinVadsUrl;
	}

	public void setPhinVadsUrl(String phinVadsUrl) {
		this.phinVadsUrl = phinVadsUrl;
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
			service = (VocabService) factory.create(VocabService.class, phinVadsUrl);
		} catch (MalformedURLException e) {
			logger.debug("Error creating client", e);
		}
		logger.debug("returning hessian client");
		return service;
	}

	private void syncCodeSystemCodes(String oid) throws Exception {
		logger.info("Syncing CodeSytemCodesfor " + oid);
		int page = 1;
		int perPage = 10000;
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
			ValueSet valueSet = iterator.next();
			logger.info("Processing Valueset " + valueSet.getName());
			updateElasticSearch("value_sets", "value_set", valueSet.getId(), objectToJSON(valueSet));
		}
	}

	public void countCodes() {
		Map<String, ValueSet> valueSets = new HashMap<String, ValueSet>();
		List<ValueSet> vsets = getService().getAllValueSets().getValueSets();
		for (Iterator<ValueSet> iterator = vsets.iterator(); iterator.hasNext();) {
			ValueSet valueSet = iterator.next();
			logger.info("Processing Valueset " + valueSet.getName());
			valueSets.put(valueSet.getOid(), valueSet);
		}
		int maxVersionNumber = 0;
		Map<String, Map<Integer, Integer>> counts = new HashMap<String, Map<Integer, Integer>>();
		List<ValueSetVersion> vsv = getService().getAllValueSetVersions().getValueSetVersions();
		for (Iterator<ValueSetVersion> iterator = vsv.iterator(); iterator.hasNext();) {
			ValueSetVersion valueSetVersion = iterator.next();
			ValueSetConceptResultDto dto = getService().getValueSetConceptsByValueSetVersionId(valueSetVersion.getId(),
					1, 10);
			logger.info("Processing Valueset " + valueSetVersion.getValueSetOid() + " "
					+ valueSetVersion.getVersionNumber() + " " + dto.getTotalResults());
			if (!counts.containsKey(valueSetVersion.getValueSetOid())) {
				counts.put(valueSetVersion.getValueSetOid(), new HashMap<Integer, Integer>());
			}
			counts.get(valueSetVersion.getValueSetOid()).put(valueSetVersion.getVersionNumber(), dto.getTotalResults());
			maxVersionNumber = maxVersionNumber < valueSetVersion.getVersionNumber()
					? valueSetVersion.getVersionNumber() : maxVersionNumber;
		}

		for (String oid : counts.keySet()) {
			Map<Integer, Integer> versions = counts.get(oid);
			ValueSet vset = valueSets.get(oid);
			StringBuffer buff = new StringBuffer();
			buff.append(vset.getName() + "," + oid);
			for (int i = 1; i <= maxVersionNumber; i++) {
				buff.append(",");
				if (versions.containsKey(i)) {
					buff.append(versions.get(i));
				}
			}
			System.out.println(buff.toString());
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
		PhinSync sync = new PhinSync();
		Options options = new Options();
		// add t option
		options.addOption("e", true, "Elastic Search URL");
		options.addOption("v", true, "PHIN VADS URL");

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);
		if (cmd.hasOption('e')) {
			sync.setEsSearchUrl(cmd.getOptionValue('e'));
		}
		if (cmd.hasOption('v')) {
			sync.setPhinVadsUrl(cmd.getOptionValue('v'));
		}

		sync.countCodes();
		// sync.syncValueSets();
		// sync.syncValueSetVersions();

	}
}
