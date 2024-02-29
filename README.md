# kafka-rewind

`kafka-rewind` is a tool that allows you to reset the offset of the kafka trigger(s) of an AWS lambda to an earlier time,
such that it can re-consume earlier events for testing.

## Developer guide

### Setup

This repository is intended to be used with yarn via corepack. If you haven't already, enable corepack using the following command;
this will install yarn automatically.

```sh
corepack enable
```

Now, you can install the project dependencies by just executing yarn:

```sh
yarn
```

### Running the program

To run the program for testing, use the `start` command. Any arguments you pass to it will be passed
on to the program.

```sh
yarn start -- <ARGS>
```

### Publishing a new version

To publish a new version, increase the version number in `package.json`, and then run:

```sh
yarn npm publish
```
