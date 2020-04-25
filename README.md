# eh-outbox: An implementation of standard outbox pattern
eh-outbox is the experimental driver of standard message outbox and message relay pattern which is useful to publish the domain events to external service as well as to preapare the different read views. It will help to decouple the write and read side, so even if readside service/db is down, still the write side will be operational.

Note: It is experimental driver and eventually will be merge to eventhorizon(https://github.com/looplab/eventhorizon) once it is stable enough.


Design:
![alt text](https://user-images.githubusercontent.com/8401256/60906995-16a1b000-a296-11e9-84de-7a5458ab0d57.png)
