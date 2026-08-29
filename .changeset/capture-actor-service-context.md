---
"@humanlayer/effect-machine": patch
---

Preserve Effect service dependencies supplied while allocating a local actor, so state effects run correctly when `actor.start` is called later.
