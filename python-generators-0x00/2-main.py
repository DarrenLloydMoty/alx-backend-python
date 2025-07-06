import sys
processing = __import__('1-batch_processing')

##### print processed users in a batch of 50
try:
    processing.batch_processing(50)
except BrokenPipeError:
    sys.stderr.close()import sys
lazy_paginator = __import__('2-lazy_paginate').lazy_pagination


try:
    for page in lazy_paginator(100):
        for user in page:
            print(user)

except BrokenPipeError:
    sys.stderr.close()