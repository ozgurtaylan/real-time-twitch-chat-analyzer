from flask import Flask, request, render_template ,redirect,url_for
from kafka import KafkaProducer , KafkaAdminClient
from threading import Thread
from time import sleep
from json import dumps
import logging
import socket
import datetime
import os
import re

emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F" #emotions
                           u"\U0001F300-\U0001F5FF" #sumbols and pictographs
                           u"\U0001F680-\U0001F6FF" #transport and map symbols
                           u"\U0001F1E0-\U0001F1FF" #flags
                           u"\U00002702-\U000027B0"  
                           u"\U000024C2-\U0001F251" 
                           "]+", flags=re.UNICODE)

# ENVIRONMENT VARIABLES 
SERVER = str(os.environ['MY_SERVER'])
PORT = int(os.environ['MY_PORT'])
NICKNAME = str(os.environ['MY_NICKNAME'])
TOKEN = str(os.environ['MY_AUTH'])

SESSION = False
producer = None

app = Flask(__name__)

@app.route("/",methods=["POST","GET"])
def main_page():
    if request.method == "POST":
        username = request.form["username"]
        if username != "":
            channel = "#"+username
            background_thread = Thread(target=start_session , args=(channel,),daemon=True)
            background_thread.start()
            sleep(2)
            return redirect(url_for('control_page',username=username))
            
    else:
        return render_template("main_page.html")

@app.route("/control/<string:username>",methods=["POST","GET"])
def control_page(username):
    if request.method == "POST":
        if "go_dashboard" in request.form:
            return redirect("kibana:5601")
        elif "end_session" in request.form:
            end_session()
            ## Close everthink
            return redirect(url_for('goodbye'))

    return render_template("control_page.html",username=username)

@app.route("/goodbye",methods=["GET"])
def goodbye():
    return render_template("goodbye.html")

@app.route("/error",methods=["GET"])
def error():
    return render_template("error.html")

def kafka_producer_start():
    global producer
    producer = KafkaProducer(
    bootstrap_servers=['broker:29092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

def start_session(username):
    
    sock = socket.socket()
    try:        
        kafka_producer_start()
        sock.connect((SERVER, PORT))
        sock.send(f"PASS {TOKEN}\n".encode('utf-8'))
        sock.send(f"NICK {NICKNAME}\n".encode('utf-8'))
        sock.send(f"JOIN {username}\n".encode('utf-8'))
        
        welcoming_resp = sock.recv(2048).decode('utf-8')
        #print(welcoming_resp)
        join_resp = sock.recv(2048).decode('utf-8')
        #print(join_resp)
        ### Join Valid Check?
        i = 0
        global SESSION 
        SESSION = True
        while SESSION:
            resp = sock.recv(2048).decode('utf-8')
            if resp.startswith('PING'):
                sock.send("PONG\n".encode('utf-8'))
            elif len(resp) > 0:
                resp_list = resp.split(':')
                if len(resp_list) >= 3:
                    kafka_val_chat_message = (resp_list[2]).replace("\r\n","")
                    kafka_val_chat_message = emoji_pattern.sub(r'', kafka_val_chat_message)
                    kafka_val_chat_message= re.sub(r'http\S+', '', kafka_val_chat_message)
                    if kafka_val_chat_message != "":

                        now = datetime.datetime.now()
                        kafka_val_time = now.strftime("%H:%M:%S")
                        kafka_val_order = i
                        kafka_val_username = resp_list[1].split('!')[0]
                        kafka_val_streamer = username
                        
                        kafka_whole_data = {
                            "streamer":kafka_val_streamer,
                            "order":kafka_val_order,
                            "time":kafka_val_time,
                            "username":kafka_val_username,
                            "message":kafka_val_chat_message
                        }
                        if producer is not None:
                            producer.send(topic="twitch-message",value=kafka_whole_data)
                            app.logger.info(f"SENT {kafka_whole_data}")
                        else:
                            error = 1
                            app.logger.error('Error Occured!')
                        i+=1
          
        sock.close()
        app.logger.info("Socket closed..")
        app.logger.info("Thread leaving..")
        return

    except Exception as e:
        error=1
        app.logger.error(f'Error Occured! {e}')
        app.logger.error('Socket Closing..!')    
        app.logger.error('Thread Leaving..!') 
        sock.close()
        return
    finally:
        sock.close()
        return

def end_session(): 

    global SESSION
    SESSION = False

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001 , debug=True)