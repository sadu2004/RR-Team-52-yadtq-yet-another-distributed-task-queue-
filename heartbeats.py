from yadtq import YADTQ

def main():
    yadtq = YADTQ()
    try:
        yadtq.track_heartbeats(timeout=10)
    except KeyboardInterrupt:
        print("Heartbeat monitoring stopped.")

if __name__ == '__main__':
    main()
