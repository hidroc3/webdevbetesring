import { Test, TestingModule } from '@nestjs/testing';
import { HydrologicOutputHechmsController } from './hydrologic-output-hechms.controller';

describe('HydrologicOutputHechmsController', () => {
  let controller: HydrologicOutputHechmsController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [HydrologicOutputHechmsController],
    }).compile();

    controller = module.get<HydrologicOutputHechmsController>(HydrologicOutputHechmsController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
