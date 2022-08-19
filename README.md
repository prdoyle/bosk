# Bosk
Bosk is a control plane state management system for distributed applications.
It aims to ease the transition from a single standalone Java application to a replicated cluster
with minimal surprise.

## Quick start

The `bosk-core` library is enough to get started.
Create a `Bosk` singleton object, and use it to house your application's state tree.
Use `Bosk::simpleDriver` as your driver.
Register hook functions to take the appropriate actions when parts of the state change.

Add in other packages as you need them,
like `bosk-gson` for JSON serialization.
Use the same version number for all packages.

Once you've got your application running,
you can bring in the `bosk-mongo` package,
and replace `Bosk::simpleDriver` with a `MongoDriver`.
Then run MongoDB along with one or more replicas of your application,
all configured to use the same MongoDB collection to store state.
If you've done it right, you now have a replica set of servers all sharing the same state!

## Development

### Code Structure

The repo is structured as a collection of subprojects because we publish several separate libraries.
`bosk-core` is the main functionality, and then other packages like `bosk-mongo` and `bosk-gson`
provide integrations with other technologies.

The subprojects are listed in `settings.gradle`, and each has its own `README.md` describing what it is.

### Maven publishing

During development, set `version` in `build.gradle` to end with `-SNAPSHOT` suffix.
When you're ready to make a release, just delete the `-SNAPSHOT`.
The next commit should then bump the version number and re-add the `-SNAPSHOT` suffix
to begin development of the next version.

If you'd like to publish a snapshot version, you can comment out this code from `Jenkinsfile` like so:

```groovy
    stage('Publish to Artifactory') {
//      when {
//          branch 'develop'
//      }
```

### Versioning

In the long run, we'll use the usual semantic versioning.

For the 0.x.y releases, treat x as a manjor release number.

For the 0.0.x releases, all bets are off, and no backward compatibility is guaranteed.
