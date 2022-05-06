/*
 * Copyright (c) 2022 Fraunhofer IWU.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.linkedfactory.kvin.cli

import io.github.linkedfactory.kvin.Kvin
import io.github.linkedfactory.kvin.leveldb.KvinLevelDb

import java.nio.file.{Files, Paths}

/**
 * Interface for a command that can be executed through the standard
 * command-line-interface against a {@link Kvin} instance.
 */
trait CLICommand {
  def description: String

  def name: String

  def run(kvin: Kvin, args: String*)

  def usage: String
}

/**
 * A command-line interface for a {@link Kvin} store.
 */
class CLI(args: Array[String]) {
  def showCommands {
    System.out.println("Available commands:");
    commands.values.foreach { cmd =>
      System.out.print(String.format("  %1$-15s", cmd.name))
      val desc = cmd.description
      System.out.println(if (desc != null && desc.length() > 0) "  " + desc else "")
    }
  }

  class HelpCommand extends CLICommand {
    override def run(kvin: Kvin, args: String*) {
      if (args.length > 0) {
        commands.get(args(0)) match {
          case Some(cmd) =>
            val desc = cmd.description
            if (desc != null && desc.length() > 0) {
              System.out.println(desc);
            }
            System.out.println(cmd.usage)
          case None => System.err.println("Unknown command '" + args(0) + "'")
        }
      } else {
        System.err.println("Usage: " + usage);
      }
    }

    override def name = "help"

    override def usage = name + " command"

    override def description = "Display usage information for a command."
  }

  private val commands: Map[String, CLICommand] = List(new ItemsCmd(), //
    new PropertiesCmd(), //
    new FetchCmd(), //
    new HelpCommand()).map { cmd => (cmd.name, cmd) }.toMap

  args.toList match {
    case kvinLocation :: rest =>
      rest match {
        case cmd :: cmdArgs => commands.get(cmd) orElse {
          System.err.println("Unknown command: " + cmd)
          showCommands
          None
        } foreach { cmd =>
          val path = Paths.get(kvinLocation)
          if (!Files.exists(path)) {
            System.err.println("Database not found at location: " + kvinLocation)
            System.exit(1)
          }

          val kvin = new KvinLevelDb(path.toFile());
          try {
            cmd.run(kvin, cmdArgs.toArray: _*)
          } finally {
            kvin.close
          }
        }
        case _ => showCommands
      }
    case _ => System.err.println("Location of storage folder is required.");
  }
}

object CLI extends App {
  new CLI(args)
}