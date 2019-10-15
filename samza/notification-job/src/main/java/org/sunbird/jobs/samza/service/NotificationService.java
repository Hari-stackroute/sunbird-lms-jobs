package org.sunbird.jobs.samza.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.samza.config.Config;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.jobs.samza.util.JSONUtils;
import org.sunbird.jobs.samza.util.JobLogger;
import org.sunbird.jobs.samza.util.NotificationEnum;
import org.sunbird.notification.beans.EmailConfig;
import org.sunbird.notification.beans.EmailRequest;
import org.sunbird.notification.beans.SMSConfig;
import org.sunbird.notification.email.service.IEmailFactory;
import org.sunbird.notification.email.service.IEmailService;
import org.sunbird.notification.email.service.impl.IEmailProviderFactory;
import org.sunbird.notification.fcm.provider.IFCMNotificationService;
import org.sunbird.notification.fcm.provider.NotificationFactory;
import org.sunbird.notification.fcm.providerImpl.FCMHttpNotificationServiceImpl;
import org.sunbird.notification.sms.provider.ISmsProvider;
import org.sunbird.notification.utils.FCMResponse;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.sunbird.jobs.samza.util.Constant;
import org.sunbird.notification.utils.SMSFactory;

/**
 * 
 * @author manzarul
 *
 */
public class NotificationService {
	ObjectMapper mapper = new ObjectMapper();
	private JobLogger Logger = new JobLogger(NotificationService.class);
	private Config appConfig = null;
	private String accountKey = null;
	private String smsAuthKey = null;
	private String smsDefaultSender = null;
	private ISmsProvider smsProvider = null;
	private IEmailFactory emailFactory = null;
	private IEmailService emailService = null;
	private IFCMNotificationService ifcmNotificationService = NotificationFactory
			.getInstance(NotificationFactory.instanceType.httpClinet.name());

	public void initialize(Config config) throws Exception {
		JSONUtils.loadProperties(config);
		appConfig = config;
		accountKey = appConfig.get(NotificationEnum.fcm_account_key.name());
		smsAuthKey = appConfig.get(NotificationEnum.sms_auth_key.name());
		smsDefaultSender = appConfig.get(NotificationEnum.sms_default_sender.name());
		SMSConfig smsConfig = new SMSConfig(smsAuthKey, smsDefaultSender);
		smsProvider = SMSFactory.getInstance("91SMS", smsConfig);
		emailFactory = new IEmailProviderFactory();
		emailService = emailFactory.create(
		        new EmailConfig(appConfig.get(NotificationEnum.mail_server_from_email.name()),
										appConfig.get(NotificationEnum.mail_server_username.name()),
                    appConfig.get(NotificationEnum.mail_server_password.name()),
										appConfig.get(NotificationEnum.mail_server_host.name()),
										appConfig.get(NotificationEnum.mail_server_port.name())));
		Logger.info("NotificationService:initialize: Service config initialized");
	}

	public void processMessage(Map<String, Object> message) throws Exception {
		Logger.info("Account key:"+ accountKey);
		FCMHttpNotificationServiceImpl.setAccountKey(accountKey);
		String msgId = (String) message.get(Constant.MID);
		Map<String, Object> edataMap = (Map<String, Object>) message.get(Constant.EDATA);
		Map<String, Object> objectMap = (Map<String, Object>) message.get(Constant.OBJECT);
		String requestHash = "";
		boolean isSuccess = false;
		if (edataMap != null && edataMap.size() > 0) {
			String actionValue = (String) edataMap.get(Constant.ACTION);
			if (Constant.ACTION_NAME.equalsIgnoreCase(actionValue)) {
				Map<String, Object> requestMap = (Map<String, Object>) edataMap.get(Constant.REQUEST);
				requestHash = OneWayHashing.encryptVal(mapper.writeValueAsString(requestMap));
				if (!requestHash.equals((String) objectMap.get(Constant.ID))) {
					Logger.info("NotificationService:processMessage: hashValue is not matching - " + requestHash);
				} else {
					Map<String, Object> notificationMap = (Map<String, Object>) requestMap.get(Constant.NOTIFICATION);
					if(notificationMap.get(Constant.MODE).equals(NotificationEnum.phone.name())) {
						isSuccess = sendSmsNotification(notificationMap, msgId);
					} else if(notificationMap.get(Constant.MODE).equals(NotificationEnum.email.name())){
						isSuccess = sendEmailNotification(notificationMap);
					} else if(notificationMap.get(Constant.MODE).equals(NotificationEnum.device.name())) {
						isSuccess = notifyDevice(notificationMap);
					}
					if (isSuccess) {
						Logger.info("Notification sent successfully.");
					} else {
						Logger.info("Notification sent failure");
					}
				}
			} else {
				Logger.info("NotificationService:processMessage action name is incorrect: " + actionValue + "for message id:"+msgId);
			}
		} else {
			Logger.info("NotificationService:processMessage event data map is either null or empty for message id:"+msgId);
		}
	}

	private boolean sendEmailNotification(Map<String, Object> notificationMap) {
		List<String> deviceIds = (List<String>) notificationMap.get(Constant.IDS);
		Map<String, Object> templateMap = (Map<String, Object>) notificationMap.get(Constant.TEMPLATE);
		Map<String, Object> config =  (Map<String, Object>) notificationMap.get(Constant.CONFIG);
		String subject = (String) config.get(Constant.SUBJECT);
		String emailText = (String) templateMap.get(Constant.DATA);
		EmailRequest emailRequest = new EmailRequest(subject, deviceIds,null,null,"", emailText,null);
		return emailService.sendEmail(emailRequest);
	}

	private boolean sendSmsNotification(Map<String, Object> notificationMap, String msgId) {
		List<String> deviceIds = (List<String>) notificationMap.get(Constant.IDS);
		if (deviceIds != null) {
			Map<String, Object> templateMap = (Map<String, Object>) notificationMap.get(Constant.TEMPLATE);
			String smsText = (String) templateMap.get(Constant.DATA);
			return smsProvider.bulkSms(deviceIds, smsText);
		} else {
			Logger.info("mobile numbers not provided for message id:"+msgId);
			return true;
		}
	}

	private boolean notifyDevice(Map<String, Object> notificationMap) throws JsonProcessingException {
		String topic = null;
		FCMResponse response = null;
		List<String> deviceIds = (List<String>) notificationMap.get(Constant.IDS);
		Map<String, String> dataMap = new HashMap<String, String>();
		dataMap.put(Constant.RAW_DATA, mapper.writeValueAsString(notificationMap.get(Constant.RAW_DATA)));
		Logger.info("NotificationService:processMessage: calling send notification ");
		if (deviceIds != null ) {
			response = ifcmNotificationService.sendMultiDeviceNotification(deviceIds, dataMap, false);
		} else {
			Map<String, Object> configMap = (Map<String, Object>) notificationMap.get(Constant.CONFIG);
			topic = (String) configMap.getOrDefault(Constant.TOPIC, "");
			response = ifcmNotificationService.sendTopicNotification(topic, dataMap, false);
		}
		if (response != null) {
			Logger.info("Send device notiifcation response with canonicalId,ErrorMsg,successCount,FailureCount"
							+ response.getCanonical_ids() + "," + response.getError() + ", " + response.getSuccess() + " "
							+ response.getFailure());
			return true;
		} else {
			Logger.info("response is improper from fcm:" + response + "for device ids" + deviceIds + "or topic"+ topic);
			return false;
		}
	}

}
