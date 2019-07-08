# eh-outbox
eh-outbox is the experimental driver of standard message outbox and message relay pattern which is useful to publish the domain events to external service as well as to preapare the different read views. It will help to decouple the write and read side, so even if readside service/db is down, still the write side will be operational.

Note: It is experimental driver and eventually will be merge to eventhorizon(https://github.com/looplab/eventhorizon) once it is stable enough.


Design:
![alt text](https://user-images.githubusercontent.com/8401256/60830785-a0d60f80-a1d5-11e9-9040-55f45f33c5e5.png)
