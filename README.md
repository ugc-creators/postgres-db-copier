# Golang DB Copier

Just a quick script for copying postgres data and using transformers to remove/replace data in a second database.


**Example Config**

```yml
from_database:
  name: database1
  user: postgres
  password: password123
  host: localhost
  port: 5432

to_database:
  name: database2
  user: postgres
  password: password123
  host: localhost
  port: 5432

tables:
  - name: users
    column_transforms:
      email:
        type: email
```
