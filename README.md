# kafka-rewind

`kafka-rewind` is a tool that allows you to reset the offset of the kafka trigger(s) of an AWS lambda to an earlier time,
such that it can re-consume earlier events for testing.

## Installation guide

To install kafka-rewind, first, make sure you have the otto-ec npm registry configured.

### Configuring the otto-ec npm registry

First, make sure you set the `GITHUB_TOKEN` environment variable to a github access token that has
access to `read:packages` within the otto-ec organization.

Afterwards, make sure the following lines are included in your `~/.npmrc` file:

```.npmrc
@otto-ec:registry=https://npm.pkg.github.com/
//npm.pkg.github.com/:_authToken=${GITHUB_TOKEN}
```

### Installing the package

To install the package, simply run the following command:

```sh
npm install --global @otto-ec/kakfa-rewind
```

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
