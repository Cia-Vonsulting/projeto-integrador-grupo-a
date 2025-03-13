def lambda_handler(event, context):
    try:
        print(event)

    except Exception as e:
        print(f'e: {e}')
