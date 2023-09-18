import logging
import azure.functions as func
from azure.communication.email import EmailClient
import json

connection_string = "endpoint=https://alfaeyecomunication.unitedstates.communication.azure.com/;accesskey=hzr4gArVwmWc4G7B+CVMxruksrvsJdD2TOvsgU3ziNTpPv2vToa1AVREEjOADodtHxByjdFPwW37kTjLvidDvQ=="
sender_address = "DoNotReply@8086b202-ac0d-482e-aea0-747914a81b9a.azurecomm.net"


def process_recipients(email_str):
    return [dict(address=email) for email in email_str.split(",")] if email_str else []


def main(msg: func.QueueMessage) -> None:
    logging.info(f"Sending...")
    try:
        message_body = msg.get_body().decode('utf-8')
        email_data = json.loads(message_body)

        required_keys = ['html', 'subject', 'recipients']
        if not all(key in email_data for key in required_keys):
            raise ValueError('Malformed email')

        to = process_recipients(email_data.get('recipients'))
        cc = process_recipients(email_data.get('cc_recipients'))
        bcc = process_recipients(email_data.get('bcc_recipients'))

        msg_content = {
            "subject": email_data['subject'],
            "html": email_data['html']
        }

        recipients = {"to": to}
        if cc:
            recipients["cc"] = cc
        if bcc:
            recipients["bcc"] = bcc

        client = EmailClient.from_connection_string(connection_string)
        message = dict(
            senderAddress=sender_address,
            content=msg_content,
            recipients=recipients
        )

        poller = client.begin_send(message)
        logging.info(f"Successfully: (operation id: {poller.result()['id']})")

    except Exception as e:
        logging.error(f'Failed to send email. Error: {str(e)}')
