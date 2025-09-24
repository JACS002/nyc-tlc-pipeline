import os
for k, v in sorted(os.environ.items()):
    if k.startswith('SNOWFLAKE_'):
        mask = v if ('PASSWORD' not in k) else '***'
        print(f'{k} = {mask}')
