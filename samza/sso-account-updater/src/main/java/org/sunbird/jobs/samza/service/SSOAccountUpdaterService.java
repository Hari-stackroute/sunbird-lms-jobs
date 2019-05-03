package org.sunbird.jobs.samza.service;

import com.google.gson.Gson;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.samza.config.Config;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.jobs.samza.util.JobLogger;
import org.sunbird.jobs.samza.util.JSONUtils;
import org.sunbird.jobs.samza.util.SSOAccountUpdaterParams;

import okhttp3.OkHttpClient;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okhttp3.Request;
import okhttp3.Response;

import javax.ws.rs.core.HttpHeaders;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

public class SSOAccountUpdaterService {

    private static MediaType jsonMediaType = MediaType.parse(javax.ws.rs.core.MediaType.APPLICATION_JSON);
    private JobLogger Logger = new JobLogger(SSOAccountUpdaterService.class);
    private Config appConfig = null;
    private OkHttpClient client = new OkHttpClient();
    private Gson gson = new Gson();

    public void initialize(Config config) throws Exception {
        JSONUtils.loadProperties(config);
        appConfig = config;
        Logger.info("SSOAccountUpdaterService:initialize: Service config initialized");
    }

    public void processMessage(Map<String, Object> message) throws Exception {

        Map<String, Object> eventMap = (Map<String, Object>) message.get(SSOAccountUpdaterParams.event.name());

        String userExternalId = (String) eventMap.get(SSOAccountUpdaterParams.userExternalId.name());
        String nameFromPayload = (String) eventMap.get(SSOAccountUpdaterParams.nameFromPayload.name());
        String channel = (String) eventMap.get(SSOAccountUpdaterParams.channel.name());
        String orgExternalId = (String) eventMap.get(SSOAccountUpdaterParams.orgExternalId.name());
        List<String> passedRoles = (List<String>) eventMap.get(SSOAccountUpdaterParams.roles.name());
        String userId = (String) eventMap.get(SSOAccountUpdaterParams.userId.name());
        List<Map<String, Object>> organisations = (List<Map<String, Object>>) eventMap.get(SSOAccountUpdaterParams.organisations.name());
        String firstName = (String) eventMap.get(SSOAccountUpdaterParams.firstName.name());

        Logger.info("SSOAccountUpdaterService:processMessage: Processing user data for userId - " + userId);

        Map<String, Object> passedOrg = getOrg(channel, orgExternalId);
        if (MapUtils.isEmpty(passedOrg)) {
            ProjectCommonException.throwClientErrorException(ResponseCode.invalidOrgData);
        }

        Map<String, Object> updateMap = new HashMap<>();
        if (!StringUtils.equals(nameFromPayload, firstName)) {
            updateMap.put(SSOAccountUpdaterParams.firstName.name(), nameFromPayload);
        }

        if (CollectionUtils.isEmpty(passedRoles)) {
            passedRoles = new ArrayList<String>();
            passedRoles.add(SSOAccountUpdaterParams.PUBLIC.name());
        }

        String passedOrgId = (String) passedOrg.get(SSOAccountUpdaterParams.identifier.name());
        if (CollectionUtils.isEmpty(organisations)) {
            updateMap.put(SSOAccountUpdaterParams.organisations.name(), getOrganisationsListFromPayloadData(passedOrgId, userId, passedRoles));
        } else {
            boolean isUserUpdateRequired = true;
            passedRoles.remove(SSOAccountUpdaterParams.PUBLIC.name());

            for (Map<String, Object> organisation : organisations) {
                String linkedOrgId = (String) organisation.get(SSOAccountUpdaterParams.organisationId.name());
                //find linked org
                if (StringUtils.equalsIgnoreCase(linkedOrgId, passedOrgId)) {
                    //match roles
                    List<String> assignedRoles = (List<String>) organisation.get(SSOAccountUpdaterParams.roles.name());
                    assignedRoles.remove(SSOAccountUpdaterParams.PUBLIC.name());
                    if (CollectionUtils.isEqualCollection(passedRoles, assignedRoles)) {
                        isUserUpdateRequired = false;
                        break;
                    }
                }
            }

            if (isUserUpdateRequired) {
                updateMap.put(SSOAccountUpdaterParams.organisations.name(), getOrganisationsListFromPayloadData(passedOrgId, userId, passedRoles));
            }
        }

        if (MapUtils.isNotEmpty(updateMap)) {
            updateMap.put(SSOAccountUpdaterParams.userId.name(), userId);

            Map<String, Object> updateUserRequest = new HashMap<>();
            updateUserRequest.put(SSOAccountUpdaterParams.request.name(), updateMap);

            updateUser(updateUserRequest);
            Logger.info("SSOAccountUpdaterService:processMessage: User data updated in the system for userId - " + userId);
        }

    }

    private List<Map<String, Object>> getOrganisationsListFromPayloadData(String passedOrgId, String userId, List<String> passedRoles) {
        Map<String, Object> organisationMap = new HashMap<>();
        organisationMap.put(SSOAccountUpdaterParams.organisationId.name(), passedOrgId);
        organisationMap.put(SSOAccountUpdaterParams.userId.name(), userId);
        organisationMap.put(SSOAccountUpdaterParams.roles.name(), passedRoles);

        List<Map<String, Object>> linkedOrgList = new ArrayList<>();
        linkedOrgList.add(organisationMap);

        return linkedOrgList;
    }

    private Map<String, Object> getOrg(String channel, String orgExternalId) throws Exception {

        Map<String, String> filterMap = new HashMap<>();
        filterMap.put(SSOAccountUpdaterParams.channel.name(), channel);
        filterMap.put(SSOAccountUpdaterParams.externalId.name(), orgExternalId);

        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(SSOAccountUpdaterParams.filters.name(), filterMap);

        Map<String, Object> orgSearchRequest = new HashMap<>();
        orgSearchRequest.put(SSOAccountUpdaterParams.request.name(), requestMap);

        RequestBody body = RequestBody.create(jsonMediaType, gson.toJson(orgSearchRequest));
        Request request = new Request.Builder()
                .url(appConfig.get(SSOAccountUpdaterParams.lms_host.name()) + appConfig.get(SSOAccountUpdaterParams.org_search_api.name()))
                .post(body)
                .addHeader(HttpHeaders.ACCEPT, javax.ws.rs.core.MediaType.APPLICATION_JSON)
                .addHeader(HttpHeaders.CONTENT_TYPE, javax.ws.rs.core.MediaType.APPLICATION_JSON)
                .build();

        Response response = client.newCall(request).execute();

        int responseCode = response.code();
        String responseJson = response.body().string();

        if (200 == responseCode) {
            Map<String, Object> orgSearchResponse = gson.fromJson(responseJson, Map.class);
            Map<String, Object> resultMap = (Map<String, Object>) orgSearchResponse.get(SSOAccountUpdaterParams.result.name());
            Map<String, Object> responseMap = (Map<String, Object>) resultMap.get(SSOAccountUpdaterParams.response.name());
            Double dCount = (Double) responseMap.get(SSOAccountUpdaterParams.count.name());
            int count = dCount.intValue();
            if (0 != count) {
                List<Map<String, Object>> orgs = (List<Map<String, Object>>) responseMap.get(SSOAccountUpdaterParams.content.name());
                return orgs.get(0);
            }
        }

        return new HashMap<>();

    }

    private boolean updateUser(Map<String, Object> updateUserRequest) throws Exception {

        RequestBody body = RequestBody.create(jsonMediaType, gson.toJson(updateUserRequest));
        Request request = new Request.Builder()
                .url(appConfig.get(SSOAccountUpdaterParams.lms_host.name()) + appConfig.get(SSOAccountUpdaterParams.user_update_private_api.name()))
                .patch(body)
                .addHeader(HttpHeaders.ACCEPT, javax.ws.rs.core.MediaType.APPLICATION_JSON)
                .addHeader(HttpHeaders.CONTENT_TYPE, javax.ws.rs.core.MediaType.APPLICATION_JSON)
                .build();

        Response response = client.newCall(request).execute();

        int responseCode = response.code();
        response.close();

        if (200 == responseCode) {
            return true;
        } else {
            return false;
        }

    }
}
