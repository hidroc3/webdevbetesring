import { Test, TestingModule } from '@nestjs/testing';
import { AwsLogManualsController } from './aws-log-manuals.controller';

describe('AwsLogManualsController', () => {
  let controller: AwsLogManualsController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [AwsLogManualsController],
    }).compile();

    controller = module.get<AwsLogManualsController>(AwsLogManualsController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
