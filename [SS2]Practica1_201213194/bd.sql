SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
CREATE SCHEMA IF NOT EXISTS `dwrm` DEFAULT CHARACTER SET utf8 ;
USE `dwrm` ;
CREATE TABLE IF NOT EXISTS `dwrm`.`artista` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `nombre` VARCHAR(250) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `idArtista_UNIQUE` (`id` ASC) VISIBLE)
ENGINE = InnoDB;
CREATE TABLE IF NOT EXISTS `dwrm`.`genero` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `nombre` VARCHAR(150) NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE)
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `dwrm`.`cancion` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `genero_id` INT NULL,
  `duration_ms` VARCHAR(45) NOT NULL,
  `explicit` VARCHAR(45) NOT NULL,
  `vyear` INT NOT NULL,
  `popularity` DECIMAL(10,4) NOT NULL,
  `danceability` DECIMAL(10,4) NOT NULL,
  `energy` DECIMAL(10,4) NOT NULL,
  `ekey` INT NOT NULL,
  `loudness` DECIMAL(10,4) NOT NULL,
  `modo` DECIMAL(10,4) NOT NULL,
  `speechiness` DECIMAL(10,4) NOT NULL,
  `acousticness` FLOAT(12,9) NOT NULL,
  `instrumentalness` FLOAT(12,9) NOT NULL,
  `liveness` DECIMAL(10,4) NOT NULL,
  `valence` DECIMAL(10,4) NOT NULL,
  `tempo` DECIMAL(10,4) NOT NULL,
  `nombre` VARCHAR(250) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
  INDEX `fk_cancion_genero1_idx` (`genero_id` ASC) VISIBLE,
  CONSTRAINT `fk_cancion_genero1`
    FOREIGN KEY (`genero_id`)
    REFERENCES `dwrm`.`genero` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;
CREATE TABLE IF NOT EXISTS `dwrm`.`reproduccion_musical` (
  `artista_id` INT NOT NULL,
  `cancion_id` INT NOT NULL,
  `num_reproducciones` INT NOT NULL DEFAULT 1,
  PRIMARY KEY (`artista_id`, `cancion_id`),
  INDEX `fk_reproduccion_musical_cancion1_idx` (`cancion_id` ASC) VISIBLE,
  CONSTRAINT `fk_reproduccion_musical_artista`
    FOREIGN KEY (`artista_id`)
    REFERENCES `dwrm`.`artista` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_reproduccion_musical_cancion1`
    FOREIGN KEY (`cancion_id`)
    REFERENCES `dwrm`.`cancion` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;
SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;